import asyncio
from urllib.parse import urlencode

import httpx
from sqlalchemy.orm import Session

from . import guangya_auth
from .config import settings
from .utils import RateLimiter, retry_after_seconds, traceparent

_limiter = RateLimiter()

# 每个业务接口的每秒限额（OpenAPI v1.3 §10）
_RATE = {
    "/openapi/v1/file/get_file_list": 5,
    "/openapi/v1/file/get_file_detail": 2,
    "/openapi/v1/file/get_res_download_url": 2,
    "/openapi/v1/file/get_vod_download_url": 2,
    "/openapi/v1/user/get_user_info": 2,
    "/openapi/v1/file/create_dir": 2,
    "/openapi/v1/file/rename": 2,
    "/openapi/v1/file/move_file": 2,
    "/openapi/v1/file/get_task_status": 5,
}


class GuangYaError(RuntimeError):
    def __init__(self, message: str, code: int | None = None):
        super().__init__(message)
        self.code = code


class GuangYaOpenAPI:
    """业务接口统一入口：x-client-id + Bearer Token，117 自动刷新重试一次。"""

    def __init__(self, db: Session):
        self.db = db
        self.base = settings.guangya_api_base.rstrip("/")

    async def _call(self, method: str, path: str, *, params: dict | None = None,
                    body: dict | None = None, allow_117_retry: bool = True) -> dict:
        limit = _RATE.get(path, 2)
        await _limiter.wait(path, 1.0 / limit)
        from . import runtime_config
        from .models import AuthToken
        t = self.db.get(AuthToken, 1)
        client_id = runtime_config.get(self.db, "guangya_client_id")
        # 前置防护：网页端短信登录凭证不属于官方 OpenAPI 体系，硬调必然 code=120 缺少clientId。
        # v0.5.3 已把扫描切到网页端；这里拦住其余残留调用（如 get_user_info 验证）。
        if t and t.flow in ("sms", "tgto", "webqr") and not client_id:
            raise GuangYaError(
                "当前使用网页端短信登录凭证，不能调用官方 OpenAPI 业务接口："
                "官方要求开放平台分配的 x-client-id（应用 ID），错误码 120 缺少clientId。"
                "扫描、建分享、连接验证已自动改用网页端接口，无需申请应用。", 120)
        token = await guangya_auth.get_access_token(self.db)
        headers = {
            "Authorization": f"Bearer {token}",
            "traceparent": traceparent(),
        }
        if client_id:
            headers["x-client-id"] = client_id
        if body is not None:
            headers["Content-Type"] = "application/json"
        for attempt in range(2):
            async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
                r = await client.request(method, f"{self.base}{path}", params=params,
                                         json=body, headers=headers)
            if r.status_code == 429:
                wait = retry_after_seconds(r.headers.get("Retry-After")) or 2 ** attempt
                await asyncio.sleep(wait)
                continue
            payload = self._json(r)
            code = payload.get("code")
            if code == 117 and allow_117_retry:
                try:
                    token = await guangya_auth.refresh(self.db, force=True)
                except guangya_auth.AuthError as exc:
                    raise GuangYaError(f"Access Token 无效且刷新失败：{exc}", 117) from exc
                headers["Authorization"] = f"Bearer {token}"
                continue
            if r.status_code >= 400 or (code is not None and code != 0):
                raise GuangYaError(f"光鸭接口 {path} 失败 code={code} msg={payload.get('msg') or r.text[:200]}", code)
            return payload
        raise GuangYaError(f"光鸭接口 {path} 重试后仍失败", code)

    async def list_files(self, parent_id: str = "", page: int = 0, page_size: int = 100) -> dict:
        params = {"page": page, "pageSize": page_size}
        if parent_id:
            params["parentId"] = parent_id
        payload = await self._call("GET", "/openapi/v1/file/get_file_list", params=params)
        return payload.get("data") or {}

    async def get_file_detail(self, file_id: str) -> dict:
        payload = await self._call("GET", "/openapi/v1/file/get_file_detail",
                                   params={"fileId": file_id})
        return payload.get("data") or {}

    async def get_user_info(self) -> dict:
        payload = await self._call("GET", "/openapi/v1/user/get_user_info")
        return payload.get("data") or {}

    @staticmethod
    def _json(r: httpx.Response) -> dict:
        try:
            data = r.json()
            return data if isinstance(data, dict) else {"raw": data}
        except Exception:
            return {"msg": r.text[:500], "http_status": r.status_code}


# 兼容旧 import
GuangYaClient = GuangYaOpenAPI


class WebFileError(RuntimeError):
    """网页端文件接口错误。401/登录失效时 code 用于上层判定是否需重新短信登录。"""
    def __init__(self, message: str, code: int | None = None):
        super().__init__(message)
        self.code = code


class GuangYaWebFiles:
    """网页端文件列表客户端：复用短信登录凭证（Bearer + did + dt），无需开放平台 client_id。

    接口来自光鸭网页前端：POST https://api.guangyapan.com/userres/v1/file/get_file_list
    body {page,pageSize,parentId,...}；根目录 parentId 为 "*"。返回 data.list / data.total。
    这样普通用户只需短信登录即可扫描，不必申请开放平台应用。
    """

    LIST_PATH = "/userres/v1/file/get_file_list"
    # v0.8.0 起：A→B 镜像复制。以下路径与请求/响应结构来自需求文档与 v0.8.1 抓包 HAR
    # 第八章「已确认光鸭接口」，按原样实现，不推测参数。其余写接口（建目录/重命名/删除/
    # 转存）在 8.6 仍标为待验证，未在此实现。
    COPY_PATH = "/userres/v1/file/copy_file"
    TASK_PATH = "/userres/v1/get_task_status"
    # v0.8.1：用户抓包 w.har 取证的三个写接口（请求体/响应体见 docs/光鸭接口取证.md）。
    # 只按抓到的原样实现，未出现的接口（move_file / recycle_file / clear_recycle_bin 等）
    # 虽然出现在前端 JS 里，但没有请求体证据，仍然不实现。
    CREATE_DIR_PATH = "/userres/v1/file/create_dir"
    RENAME_PATH = "/userres/v1/file/rename"
    DELETE_FILE_PATH = "/userres/v1/file/delete_file"
    # v0.9.19：投稿转存（光鸭网页端真实接口，契约取自光鸭技能 api-client.mjs /
    # protocol.mjs 与 docs/光鸭接口取证.md，均非猜测）。普通短信登录凭证即可调用。
    SHARE_TOKEN_PATH = "/userres/v1/get_share_access_token"
    SHARE_PAGE_FILES_PATH = "/userres/v1/get_share_page_files_list"
    RESTORE_SHARE_PATH = "/userres/v1/restore_share"

    def __init__(self, db: Session):
        from .models import AuthToken

        t = db.get(AuthToken, 1)
        if not t or not t.access_token_enc or t.flow not in ("sms", "tgto", "webqr"):
            raise WebFileError("尚未登录光鸭，请在管理页用短信验证码或 App 扫码登录", 401)
        self.db = db
        self.base = settings.guangya_share_base.rstrip("/")
        # v0.9.15 #267：不再把 token 快照进 self.headers。一次镜像任务持续几十分钟，
        # 后台 _job_token_refresh 每 60s 刷新并替换 DB 中 access_token_enc，光鸭签发新
        # token 后旧 token 作废；运行中的对象 header 停在旧 token → create_dir/copy_file
        # 全部 code=117。改为每次请求前调 _fresh_headers() 从库里读当前凭证。

    def _fresh_headers(self) -> dict:
        """每次请求前从 DB 读最新凭证重建 header。

        用独立短会话读取：长任务的 self.db 可能因 ORM identity map 缓存旧的 AuthToken
        对象，直接 db.get 会拿到过期 token；独立会话每次读到的都是最新 DB 值。
        若 _header_override 被显式注入（旧契约测试用 object.__new__ 构造），直接返回它。
        """
        override = getattr(self, "_header_override", None)
        if override is not None:
            return override
        from .db import SessionLocal
        from .models import AuthToken
        from .security import decrypt_secret

        s = SessionLocal()
        try:
            t = s.get(AuthToken, 1)
            if not t or not t.access_token_enc or t.flow not in ("sms", "tgto", "webqr"):
                raise WebFileError("光鸭登录凭证已失效，请重新登录", 401)
            did = getattr(t, "device_id", "") or ""
            if not did:
                raise WebFileError("短信登录凭证缺少设备 ID（did），请重新登录", 401)
            return {
                "Authorization": "Bearer " + decrypt_secret(t.access_token_enc),
                "did": did,
                "dt": "4",
                "Content-Type": "application/json",
                "Origin": "https://www.guangyapan.com",
                "Referer": "https://www.guangyapan.com/",
            }
        finally:
            s.close()

    async def _refresh_token(self) -> None:
        """遇 117 强制刷新凭证。用独立短会话，避免污染长任务 session 的未提交写事务。"""
        from .db import SessionLocal
        s = SessionLocal()
        try:
            await guangya_auth.refresh(s, force=True)
        finally:
            s.close()

    @property
    def headers(self) -> dict:
        """兼容只读访问（旧测试/外部工具）。真实请求一律走 _fresh_headers 取最新值。

        v0.9.15 #267：不再是构造时快照，每次读取都从库里取当前凭证。
        若被显式赋值（旧契约测试用 object.__new__ 注入假 header），则走该 override。
        """
        if getattr(self, "_header_override", None) is not None:
            return self._header_override
        return self._fresh_headers()

    @headers.setter
    def headers(self, value: dict):
        # 仅供测试/工具注入；生产链路不赋值，请求始终经 _fresh_headers 读最新凭证
        self._header_override = value

    def _retry_headers(self) -> dict:
        """兼容旧测试/外部读取的 header 视图；真实请求一律走 _fresh_headers。"""
        return self._fresh_headers()

    async def list_files(self, parent_id: str = "", page: int = 0, page_size: int = 100) -> dict:
        # v0.8.4：与 w.har 中网页端真实请求体对齐。旧版自造的 needSubFolderStat
        # 不是抓包字段；网页端固定带 orderBy=3 / sortType=1，page 从 0 开始。
        body = {
            "page": page,
            "pageSize": page_size,
            "parentId": parent_id or "*",  # 网页端根目录标识为 "*"
            "orderBy": 3,
            "sortType": 1,
        }
        last_error = ""
        refreshed = False
        for attempt in range(3):
            headers = self._fresh_headers()  # v0.9.15 #267：每请求重建，避开 token 快照
            async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
                r = await client.post(f"{self.base}{self.LIST_PATH}", json=body, headers=headers)
            data = self._json(r)
            code = data.get("code")
            # 网页端成功约定同分享接口：code=0 或 msg=success
            if r.status_code == 200 and (code == 0 or data.get("msg") == "success"):
                return data.get("data") or {}
            msg = str(data.get("msg") or data.get("message") or r.text[:200])
            last_error = f"网页端文件列表失败 code={code} msg={msg}"
            # v0.9.15 #267：117=旧 token 作废（后台已刷新），强制刷新后重建 header 重试一次
            if code == 117 and not refreshed:
                refreshed = True
                try:
                    await self._refresh_token()
                except Exception:
                    pass  # 刷新失败继续重试循环，下一轮 _fresh_headers 取当前库内 token
                continue
            # 其它登录态失效：不再重试，交由上层提示重新登录
            if code in (401, 116, 117) or any(k in msg.lower() for k in ("login", "登录", "unauthor", "token")):
                raise WebFileError(last_error, code)
            if r.status_code >= 500 or r.status_code == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            break
        raise WebFileError(last_error)

    async def _post_web(self, path: str, body: dict, *,
                        allow_codes: tuple = (), return_envelope: bool = False) -> dict:
        """网页端写接口统一 POST，返回 data 段；沿用 list_files 的鉴权与重试约定。

        v0.9.15 #267：header 每请求重建，117 强制刷新凭证重试一次（仅一次，避免循环）。
        v0.9.19：allow_codes 允许特定业务码（如转存 204 自转存拒绝）据此判定，不误当
        网络失败；return_envelope=True 返回整个响应体（含 code/msg/data）供调用方分辨业务码。
        """
        last_error = ""
        refreshed = False
        for attempt in range(3):
            headers = self._fresh_headers()
            async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
                r = await client.post(f"{self.base}{path}", json=body, headers=headers)
            data = self._json(r)
            code = data.get("code")
            if r.status_code == 200 and (code == 0 or data.get("msg") == "success"
                                          or code in allow_codes):
                return data if return_envelope else (data.get("data") or {})
            msg = str(data.get("msg") or data.get("message") or r.text[:200])
            last_error = f"网页端 {path} 失败 code={code} msg={msg}"
            if code == 117 and not refreshed:
                refreshed = True
                try:
                    await self._refresh_token()
                except Exception:
                    pass
                continue
            if code in (401, 116, 117) or any(k in msg.lower() for k in
                                             ("login", "登录", "unauthor", "token")):
                raise WebFileError(last_error, code)
            if r.status_code >= 500 or r.status_code == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            break
        raise WebFileError(last_error)

    async def create_dir(self, parent_id: str, dir_name: str) -> dict:
        """v0.8.1 抓包取证：创建目录。

        POST /userres/v1/file/create_dir {"parentId":"...","dirName":"..."}
        → data: {fileId, fileName, parentId, resType:2, depth, ...}
        分类目录按 A 库原名创建（不改名），影视主目录按 · 约定创建（v0.8.3 起拒绝 *）。
        """
        name = str(dir_name or "").strip()
        if not name or not str(parent_id or "").strip():
            raise WebFileError("创建目录需要 parentId 与 dirName")
        data = await self._post_web(self.CREATE_DIR_PATH,
                                    {"parentId": str(parent_id), "dirName": name})
        fid = str(data.get("fileId") or "")
        if not fid:
            raise WebFileError(f"创建目录「{name}」未返回 fileId，本次同步不标记完成")
        return {"file_id": fid, "name": str(data.get("fileName") or name),
                "parent_id": str(data.get("parentId") or parent_id), "raw": data}

    async def rename_file(self, file_id: str, new_name: str) -> None:
        """v0.8.1 抓包取证：重命名。POST /userres/v1/file/rename
        {"fileId":"...","newName":"..."} → {"msg":"success"}（无 data 段）。

        v0.8.6 起镜像链路用它改 B 库单集/电影文件名（只换中文片名段）。
        **fileId 必须是 B 镜像库的文件**：A 源头库全程只读，绝不传 A 的 fileId。
        """
        if not str(file_id or "").strip() or not str(new_name or "").strip():
            raise WebFileError("重命名需要 fileId 与 newName")
        await self._post_web(self.RENAME_PATH,
                             {"fileId": str(file_id), "newName": str(new_name)})

    async def delete_files(self, file_ids: list[str]) -> str:
        """v0.8.1 抓包取证：删除文件/目录（支持批量，异步任务）。

        POST /userres/v1/file/delete_file {"fileIds":[...]} → data.taskId
        需再用 get_task_status 轮询。需求文档明确「禁止先删旧再复制」，
        因此镜像链路默认**不调用**本方法，仅清理 superseded 时由管理员显式决定。
        """
        ids = [str(x) for x in file_ids if str(x).strip()]
        if not ids:
            raise WebFileError("删除需要至少一个 fileId")
        data = await self._post_web(self.DELETE_FILE_PATH, {"fileIds": ids})
        task_id = str(data.get("taskId") or "")
        if not task_id:
            raise WebFileError("删除接口未返回 taskId，无法确认是否完成")
        return task_id

    async def copy_file(self, file_ids: list[str], parent_id: str) -> str:
        """把 A 库文件复制到 B 库目录，返回光鸭 taskId。

        已确认请求：{"fileIds": ["..."], "parentId": "..."}
        已确认响应：{"msg":"success","data":{"taskId":"..."}}
        A 库本身不被修改；复制目标 parentId 必须是 B 库目录 fileId。
        """
        ids = [str(x) for x in file_ids if str(x).strip()]
        if not ids or not str(parent_id or "").strip():
            raise WebFileError("复制需要至少一个 fileId 和目标目录 parentId")
        data = await self._post_web(self.COPY_PATH, {"fileIds": ids, "parentId": str(parent_id)})
        task_id = str(data.get("taskId") or data.get("task_id") or data.get("id") or "")
        if not task_id:
            raise WebFileError("复制接口未返回 taskId，无法查询任务状态，本次同步不标记完成")
        return task_id

    async def get_task_status(self, task_id: str) -> dict:
        """查询复制任务，原样返回 status 与 detail，含义判断交给调用方。

        v0.8.4：前端 JS 的轮询函数 p5 已给出确切语义——
          status=2 且 detail.code 非 0 → 失败（如 157 云盘空间不足）
          status=3 → 失败
          status=2 且无 detail.code / code=0 → 成功
        旧版把 data.detail 丢掉，导致「复制失败」被当成「待确认」，
        MirrorFile 永远停在 copying，下一轮同步又被当成已存在而跳过。
        """
        data = await self._post_web(self.TASK_PATH, {"taskId": str(task_id)})
        return {"status": data.get("status"),
                "detail": data.get("detail") or {},
                "raw": data}

    # ---------------- v0.9.19：用户投稿 → 光鸭原生转存到指定目录 ----------------
    async def share_access_token(self, share_id: str, code: str = "") -> str:
        """POST /userres/v1/get_share_access_token {shareId,code} → data.accessToken。

        业务码（光鸭技能 sharePost 契约，非猜测）：
          200/201/202 = 分享失效；203/209 = 提取码错误；其余非 0 = 失败。
        访问 token 只对本次转存有效，绝不写库/入日志。
        """
        sid = str(share_id or "").strip()
        if not sid:
            raise WebFileError("分享链接解析失败，缺少 shareId")
        body = {"shareId": sid}
        if code:
            body["code"] = str(code)
        resp = await self._post_web(
            self.SHARE_TOKEN_PATH, body,
            allow_codes=(200, 201, 202, 203, 209), return_envelope=True)
        biz = int(resp.get("code") or 0)
        if biz in (200, 201, 202):
            raise WebFileError(f"分享已失效（code={biz}）：{resp.get('msg') or ''}", biz)
        if biz in (203, 209):
            raise WebFileError(f"提取码错误（code={biz}）：{resp.get('msg') or ''}", biz)
        token = str((resp.get("data") or {}).get("accessToken") or "")
        if not token:
            raise WebFileError("get_share_access_token 未返回 accessToken，无法转存")
        return token

    async def share_root_files(self, access_token: str, max_items: int = 100) -> list[dict]:
        """POST /userres/v1/get_share_page_files_list 列分享根目录文件。

        游标契约（光鸭技能 shareRootIds）：首次 cursor=0，翻页用响应 data.cursor，
        data.hasMore=false 结束；根目录超过 max_items 直接拒绝（与技能上限一致，
        避免一次性转存整棵大树）。返回 [{file_id,name,size,is_dir}]。
        """
        token = str(access_token or "").strip()
        if not token:
            raise WebFileError("缺少分享 accessToken")
        out: list[dict] = []
        seen: set = set()
        cursor = 0
        while len(out) < max_items:
            if cursor in seen:
                raise WebFileError("分享文件分页游标未推进，停止以避免死循环")
            seen.add(cursor)
            data = await self._post_web(self.SHARE_PAGE_FILES_PATH, {
                "accessToken": token, "cursor": cursor,
                "pageSize": 100, "orderBy": 0, "sortType": 0})
            for it in (data.get("list") or []):
                out.append({
                    "file_id": str(it.get("fileId") or ""),
                    "name": str(it.get("fileName") or ""),
                    "size": int(it.get("fileSize") or 0),
                    "is_dir": it.get("isDir") in (True, 1, "1"),
                })
            if len(out) > max_items:
                raise WebFileError(f"分享根目录超过 {max_items} 项，请在分享端整理后再转存")
            if not data.get("hasMore"):
                break
            nxt = data.get("cursor")
            if nxt in (None, "", cursor):
                break
            cursor = nxt
        if not out:
            raise WebFileError("分享当前没有可见文件（可能尚未完成分享审核），请稍后重试")
        return out

    async def restore_share_to_dir(self, share_url: str, target_dir_id: str,
                                   code: str = "") -> dict:
        """用户投稿的光鸭分享 → 转存到指定目录（审核通过后调用）。

        真实链路（docs/光鸭接口取证.md + 光鸭技能源码，非猜测）：
          1) parse_share_reference(share_url) → shareId / code（?code=）
          2) get_share_access_token {shareId,code} → accessToken
          3) get_share_page_files_list {accessToken,cursor,...} → 根目录 fileIds
          4) restore_share {accessToken,fileIds,parentId,shareCode}
        业务码 204 = SHARE_SELF_RESTORE（当前账号自建分享不可自转存，retryable=false）。
        返回 shared_files 供上层「列目标目录按名称+大小核对」，绝不只凭提交成功标记完成。
        """
        from .contribution_links import parse_share_reference

        share_id, qcode = parse_share_reference(share_url)
        eff_code = str(code or "").strip() or qcode
        target = str(target_dir_id or "").strip()
        if not share_id:
            raise WebFileError("投稿链接不是可解析的光鸭 /s/ 分享链接")
        if not target:
            raise WebFileError("未配置投稿转存目标目录 ID")

        token = await self.share_access_token(share_id, eff_code)
        files = await self.share_root_files(token)
        file_ids = [f["file_id"] for f in files if f["file_id"]]
        if not file_ids:
            raise WebFileError("分享根目录文件缺少 fileId，无法提交转存")
        resp = await self._post_web(self.RESTORE_SHARE_PATH, {
            "accessToken": token, "fileIds": file_ids,
            "parentId": target, "shareCode": eff_code,
        }, allow_codes=(204,), return_envelope=True)
        if int(resp.get("code") or 0) == 204:
            raise WebFileError(
                f"SHARE_SELF_RESTORE：该分享由当前账号创建，不能自转存（{resp.get('msg') or ''}）", 204)
        return {
            "share_id": share_id,
            "shared_files": files,
            "target_dir_id": target,
            "raw": resp,
        }

    @staticmethod
    def _json(r: httpx.Response) -> dict:
        try:
            data = r.json()
            return data if isinstance(data, dict) else {"raw": data}
        except Exception:
            return {"msg": r.text[:300], "http_status": r.status_code}


class GuangYaShareClient:
    """网页端分享接口。凭证来自运行时配置（管理页一键登录优先，.env 兜底）。"""

    def __init__(self, db: Session | None = None):
        from . import runtime_config
        from .security import decrypt_secret

        if db is not None:
            from .models import AuthToken
            t = db.get(AuthToken, 1)
            # 短信登录用户的 token 就是网页会话凭证，优先使用（手工 .env 凭证可能已过期）
            sms_authorization = ""
            if t and t.access_token_enc and t.flow in ("sms", "tgto", "webqr"):
                sms_authorization = "Bearer " + decrypt_secret(t.access_token_enc)
            authorization = sms_authorization or runtime_config.get(db, "guangya_share_authorization")
            did = runtime_config.get(db, "guangya_share_did")
            dt = runtime_config.get(db, "guangya_share_dt") or "4"
            smid = runtime_config.get(db, "guangya_share_smid")
            if sms_authorization:
                did = did or getattr(t, "device_id", "") or ""
        else:
            authorization = settings.guangya_share_authorization
            did = settings.guangya_share_did
            dt = settings.guangya_share_dt or "4"
            smid = settings.guangya_share_smid
        if not authorization:
            raise GuangYaError("尚未登录光鸭，请在管理页用「一键连接光鸭」短信登录")
        self.base = settings.guangya_share_base.rstrip("/")
        self.headers = {
            "Authorization": authorization,
            "did": did,
            "dt": dt or "4",
            "Content-Type": "application/json",
            "Origin": "https://www.guangyapan.com",
            "Referer": "https://www.guangyapan.com/",
        }
        if smid:
            self.headers["smid"] = smid

    @staticmethod
    def config_status(db: Session | None = None) -> dict:
        from . import runtime_config
        from .models import AuthToken

        if db is not None:
            authorization = runtime_config.get(db, "guangya_share_authorization")
            did = runtime_config.get(db, "guangya_share_did")
            smid = runtime_config.get(db, "guangya_share_smid")
            sources = runtime_config.source_map(db)
            source = sources.get("guangya_share_authorization", "empty")
            t = db.get(AuthToken, 1)
            if t and t.access_token_enc and t.flow in ("sms", "tgto", "webqr"):
                source = "sms" if t.flow != "webqr" else "webqr"  # 登录凭证优先
                authorization = "Bearer ***"
                did = did or getattr(t, "device_id", "")
        else:
            authorization = settings.guangya_share_authorization
            did = settings.guangya_share_did
            smid = settings.guangya_share_smid
            source = "env" if authorization else "empty"
        source_label = {"db": "页面配置", "env": ".env 手工配置", "sms": "短信登录（自动）",
                        "webqr": "扫码登录（自动）", "empty": "未配置"}.get(source, source)
        return {
            "configured": bool(authorization),
            "base": settings.guangya_share_base,
            "did_set": bool(did),
            "smid_set": bool(smid),
            "source": source_label,
            "note": "在管理页用手机号短信登录后，扫描与建分享自动共用凭证，无需抓包",
        }

    async def post(self, endpoint: str, payload: dict) -> dict:
        last_error = ""
        for attempt in range(3):
            async with httpx.AsyncClient(timeout=settings.guangya_timeout) as client:
                r = await client.post(f"{self.base}/userres/v1/{endpoint}",
                                      json=payload, headers=self.headers)
            if r.status_code == 429 or r.status_code >= 500:
                last_error = f"HTTP {r.status_code}"
                await asyncio.sleep(2 ** attempt)
                continue
            data = self._json(r)
            if data.get("msg") == "success" or data.get("code") == 0:
                return data
            msg = str(data.get("msg") or data.get("message") or r.text[:300])
            # 会话过期通常伴随登录跳转/鉴权失败文案；不硬编码具体文案，交由使用者核对
            if any(k in msg.lower() for k in ("login", "登录", "unauthor", "token", "会话")):
                raise GuangYaError(f"网页分享会话可能已失效：{msg}。请在 NAS 本机重新抓包更新 .env", 401)
            last_error = msg
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
        raise GuangYaError(f"网页分享接口 {endpoint} 失败：{last_error}")

    async def create(self, file_id, title, duration):
        body = {"fileIds": [str(file_id)], "title": title, "validateDuration": int(duration),
                "downloadType": 1, "enableShareCode": False, "shareCode": "",
                "shareType": 0, "maxRestoreCount": 0, "trafficLimit": "0"}
        return await self.post("share_file", body)

    async def update(self, share_id, title, duration):
        body = {"id": str(share_id), "title": title, "validateDuration": int(duration),
                "shareType": 0, "downloadType": 1, "enableShareCode": False,
                "shareCode": "", "maxRestoreCount": 0, "trafficLimit": "0",
                "shareTemplate": "光鸭云盘用户给你分享了：{{filename}}，点击链接或复制整段内容，打开「光鸭APP」即可获取。\n链接：{{link}}"}
        return await self.post("update_share", body)

    async def delete(self, share_id):
        return await self.post("delete_share", {"ids": [str(share_id)]})

    @staticmethod
    def _json(r: httpx.Response) -> dict:
        try:
            data = r.json()
            return data if isinstance(data, dict) else {"raw": data}
        except Exception:
            return {"raw": r.text[:500]}
