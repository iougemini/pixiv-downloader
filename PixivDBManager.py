#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sqlite3
from typing import Iterable, List, Optional, Sequence, Tuple


class PixivDBManager:
    """
    轻量数据库层：仅服务于“关注作者 -> 作品 URL 索引”场景。

    主要表：
    - pixiv_follow_member
    - pixiv_follow_image
    - pixiv_follow_image_url
    """

    def __init__(self, root_directory: str = ".", target: str = "", timeout: float = 10.0):
        base_dir = os.path.abspath(root_directory or ".")
        db_path = str(target or "").strip() or "db.sqlite"
        if not os.path.isabs(db_path):
            db_path = os.path.abspath(os.path.join(base_dir, db_path))

        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=float(timeout), check_same_thread=False)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def createDatabase(self) -> None:
        c = self.conn.cursor()
        try:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS pixiv_follow_member (
                    member_id       INTEGER PRIMARY KEY,
                    name            TEXT,
                    member_token    TEXT,
                    avatar_url      TEXT,
                    background_url  TEXT,
                    created_date    TEXT NOT NULL DEFAULT (datetime('now')),
                    last_sync_date  TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS pixiv_follow_image (
                    image_id         INTEGER PRIMARY KEY,
                    member_id        INTEGER NOT NULL,
                    title            TEXT,
                    caption          TEXT,
                    create_date      TEXT,
                    page_count       INTEGER,
                    mode             TEXT,
                    bookmark_count   INTEGER,
                    like_count       INTEGER,
                    view_count       INTEGER,
                    last_index_date  TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY(member_id) REFERENCES pixiv_follow_member(member_id) ON DELETE CASCADE
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS pixiv_follow_image_url (
                    image_id      INTEGER NOT NULL,
                    page_index    INTEGER NOT NULL,
                    original_url  TEXT,
                    regular_url   TEXT,
                    created_date  TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_date  TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY(image_id, page_index),
                    FOREIGN KEY(image_id) REFERENCES pixiv_follow_image(image_id) ON DELETE CASCADE
                )
                """
            )

            c.execute("CREATE INDEX IF NOT EXISTS idx_follow_image_member_id ON pixiv_follow_image(member_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_follow_url_image_id ON pixiv_follow_image_url(image_id)")

            self.conn.commit()
        finally:
            c.close()

    def upsertFollowMember(
        self,
        *,
        member_id: int,
        name: Optional[str] = None,
        member_token: Optional[str] = None,
        avatar_url: Optional[str] = None,
        background_url: Optional[str] = None,
    ) -> None:
        c = self.conn.cursor()
        try:
            c.execute(
                """
                INSERT INTO pixiv_follow_member(member_id, name, member_token, avatar_url, background_url, created_date, last_sync_date)
                VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                ON CONFLICT(member_id) DO UPDATE SET
                    name=excluded.name,
                    member_token=excluded.member_token,
                    avatar_url=excluded.avatar_url,
                    background_url=excluded.background_url,
                    last_sync_date=datetime('now')
                """,
                (int(member_id), name, member_token, avatar_url, background_url),
            )
            self.conn.commit()
        finally:
            c.close()

    def upsertFollowImage(
        self,
        *,
        image_id: int,
        member_id: int,
        title: Optional[str] = None,
        caption: Optional[str] = None,
        create_date: Optional[str] = None,
        page_count: Optional[int] = None,
        mode: Optional[str] = None,
        bookmark_count: Optional[int] = None,
        like_count: Optional[int] = None,
        view_count: Optional[int] = None,
    ) -> None:
        c = self.conn.cursor()
        try:
            c.execute(
                """
                INSERT INTO pixiv_follow_image(
                    image_id, member_id, title, caption, create_date,
                    page_count, mode, bookmark_count, like_count, view_count, last_index_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(image_id) DO UPDATE SET
                    member_id=excluded.member_id,
                    title=excluded.title,
                    caption=excluded.caption,
                    create_date=excluded.create_date,
                    page_count=excluded.page_count,
                    mode=excluded.mode,
                    bookmark_count=excluded.bookmark_count,
                    like_count=excluded.like_count,
                    view_count=excluded.view_count,
                    last_index_date=datetime('now')
                """,
                (
                    int(image_id),
                    int(member_id),
                    title,
                    caption,
                    create_date,
                    int(page_count or 1),
                    mode,
                    bookmark_count,
                    like_count,
                    view_count,
                ),
            )
            self.conn.commit()
        finally:
            c.close()

    def upsertFollowImageUrls(self, image_id: int, url_rows: Sequence[Tuple[int, int, str, str]]) -> None:
        if not url_rows:
            return

        rows: List[Tuple[int, int, str, str]] = []
        for item in url_rows:
            if not item:
                continue
            if isinstance(item, (tuple, list)) and len(item) >= 4:
                _img, page_idx, ori, reg = item[0], item[1], item[2], item[3]
                rows.append((int(image_id), int(page_idx), str(ori or ""), str(reg or "")))

        if not rows:
            return

        c = self.conn.cursor()
        try:
            c.executemany(
                """
                INSERT INTO pixiv_follow_image_url(image_id, page_index, original_url, regular_url, created_date, updated_date)
                VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
                ON CONFLICT(image_id, page_index) DO UPDATE SET
                    original_url=excluded.original_url,
                    regular_url=excluded.regular_url,
                    updated_date=datetime('now')
                """,
                rows,
            )
            self.conn.commit()
        finally:
            c.close()

    def selectFollowImageIdsByMember(self, member_id: int) -> List[int]:
        c = self.conn.cursor()
        try:
            c.execute("SELECT image_id FROM pixiv_follow_image WHERE member_id = ?", (int(member_id),))
            return [int(r[0]) for r in c.fetchall()]
        finally:
            c.close()

    def selectFollowImageIdsNeedingUrlIndex(self, member_id: int) -> List[int]:
        c = self.conn.cursor()
        try:
            c.execute(
                """
                SELECT fi.image_id
                  FROM pixiv_follow_image fi
             LEFT JOIN pixiv_follow_image_url fu
                    ON fu.image_id = fi.image_id
                 WHERE fi.member_id = ?
              GROUP BY fi.image_id
                HAVING COUNT(fu.page_index) = 0
                """,
                (int(member_id),),
            )
            return [int(r[0]) for r in c.fetchall()]
        finally:
            c.close()

    def deleteFollowImage(self, image_id: int) -> None:
        c = self.conn.cursor()
        try:
            c.execute("DELETE FROM pixiv_follow_image WHERE image_id = ?", (int(image_id),))
            self.conn.commit()
        finally:
            c.close()

    def countFollowMembers(self) -> int:
        c = self.conn.cursor()
        try:
            c.execute("SELECT COUNT(*) FROM pixiv_follow_member")
            row = c.fetchone()
            return int(row[0] if row else 0)
        finally:
            c.close()

    def countFollowImagesAll(self) -> int:
        c = self.conn.cursor()
        try:
            c.execute("SELECT COUNT(*) FROM pixiv_follow_image")
            row = c.fetchone()
            return int(row[0] if row else 0)
        finally:
            c.close()

    def countFollowImageUrlsAll(self) -> int:
        c = self.conn.cursor()
        try:
            c.execute("SELECT COUNT(*) FROM pixiv_follow_image_url")
            row = c.fetchone()
            return int(row[0] if row else 0)
        finally:
            c.close()

    def exportAllUrls(self, *, kind: str = "regular") -> List[str]:
        field = "regular_url" if str(kind).lower() != "original" else "original_url"
        c = self.conn.cursor()
        try:
            c.execute(
                f"""
                SELECT {field}
                  FROM pixiv_follow_image_url
              ORDER BY image_id DESC, page_index ASC
                """
            )
            out: List[str] = []
            for row in c.fetchall():
                url = str(row[0] or "").strip()
                if url:
                    out.append(url)
            return out
        finally:
            c.close()

    def iterFollowImageUrlRowsByMember(self, member_id: int) -> Iterable[Tuple[int, int, str, str]]:
        """
        Yield (image_id, page_index, original_url, regular_url) for a member.
        """
        c = self.conn.cursor()
        try:
            c.execute(
                """
                SELECT fu.image_id, fu.page_index, fu.original_url, fu.regular_url
                  FROM pixiv_follow_image_url fu
                  JOIN pixiv_follow_image fi
                    ON fi.image_id = fu.image_id
                 WHERE fi.member_id = ?
              ORDER BY fu.image_id DESC, fu.page_index ASC
                """,
                (int(member_id),),
            )
            for row in c:
                yield (
                    int(row[0]),
                    int(row[1]),
                    str(row[2] or ""),
                    str(row[3] or ""),
                )
        finally:
            c.close()
