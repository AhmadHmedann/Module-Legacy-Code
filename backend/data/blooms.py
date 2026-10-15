import datetime

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from data.connection import db_cursor
from data.users import User


@dataclass
class Bloom:
    id: int
    sender: User
    content: str
    sent_timestamp: datetime.datetime
    original_bloom_id: Optional[int] = None
    original_sender: Optional[str] = None
    rebloom_count: int = 0


def add_bloom(
    *,
    sender: User,
    content: str,
) -> Bloom:
    hashtags = [word[1:] for word in content.split(" ") if word.startswith("#")]

    now = datetime.datetime.now(tz=datetime.UTC)
    bloom_id = int(now.timestamp() * 1000000)

    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO blooms (
                id,
                sender_id,
                content,
                send_timestamp
            )
            VALUES (
                %(bloom_id)s,
                %(sender_id)s,
                %(content)s,
                %(timestamp)s
            )
            """,
            dict(
                bloom_id=bloom_id,
                sender_id=sender.id,
                content=content,
                timestamp=now,
            ),
        )

        for hashtag in hashtags:
            cur.execute(
                """
                INSERT INTO hashtags (hashtag, bloom_id)
                VALUES (%(hashtag)s, %(bloom_id)s)
                """,
                dict(
                    hashtag=hashtag,
                    bloom_id=bloom_id,
                ),
            )


def add_rebloom(*, sender: User, bloom_id: int):
    now = datetime.datetime.now(tz=datetime.UTC)

    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO reblooms (sender_id, bloom_id, send_timestamp)
            VALUES (%(sender_id)s, %(bloom_id)s, %(timestamp)s)
            """,
            {
                "sender_id": sender.id,
                "bloom_id": bloom_id,
                "timestamp": now,
            },
        )


def get_blooms_for_user(
    username: str, *, before: Optional[int] = None, limit: Optional[int] = None
) -> List[Bloom]:
    with db_cursor() as cur:
        kwargs = {
            "sender_username": username,
        }

        if before is not None:
            before_clause = "AND send_timestamp < %(before_limit)s"
            kwargs["before_limit"] = before
        else:
            before_clause = ""

        limit_clause = make_limit_clause(limit, kwargs)

        cur.execute(
            f"""
    WITH all_blooms AS (
        SELECT
            b.id,
            u.username AS sender_username,
            b.content,
            b.send_timestamp,
            NULL::BIGINT AS original_bloom_id,
            NULL::VARCHAR AS original_sender,
            (
                SELECT COUNT(*)
                FROM reblooms r
                WHERE r.bloom_id = b.id
            ) AS rebloom_count
        FROM blooms b
        JOIN users u ON u.id = b.sender_id

        UNION ALL

        SELECT
            r.id,
            ru.username AS sender_username,
            b.content,
            r.send_timestamp,
            r.bloom_id AS original_bloom_id,
            ou.username AS original_sender,
            (
                SELECT COUNT(*)
                FROM reblooms r2
                WHERE r2.bloom_id = b.id
            ) AS rebloom_count
        FROM reblooms r
        JOIN users ru ON ru.id = r.sender_id
        JOIN blooms b ON b.id = r.bloom_id
        JOIN users ou ON ou.id = b.sender_id
    )
    SELECT
        id,
        sender_username,
        content,
        send_timestamp,
        original_bloom_id,
        original_sender,
        rebloom_count
    FROM all_blooms
    WHERE sender_username = %(sender_username)s
    {before_clause}
    ORDER BY send_timestamp DESC
    {limit_clause}
    """,
            kwargs,
        )
        rows = cur.fetchall()
        blooms = []

        for row in rows:
            (
                bloom_id,
                sender_username,
                content,
                timestamp,
                original_bloom_id,
                original_sender,
                rebloom_count,
            ) = row

            blooms.append(
                Bloom(
                    id=bloom_id,
                    sender=sender_username,
                    content=content,
                    sent_timestamp=timestamp,
                    original_bloom_id=original_bloom_id,
                    original_sender=original_sender,
                    rebloom_count=rebloom_count,
                )
            )

    return blooms


def get_bloom(bloom_id: int) -> Optional[Bloom]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT
                b.id,
                u.username AS sender_username,
                b.content,
                b.send_timestamp,
                NULL::BIGINT AS original_bloom_id,
                NULL::VARCHAR AS original_sender,
                (
                    SELECT COUNT(*)
                    FROM reblooms r
                    WHERE r.bloom_id = b.id
                ) AS rebloom_count
            FROM blooms b
            JOIN users u ON u.id = b.sender_id
            WHERE b.id = %(bloom_id)s

            UNION ALL

            SELECT
                r.id,
                ru.username AS sender_username,
                b.content,
                r.send_timestamp,
                r.bloom_id AS original_bloom_id,
                ou.username AS original_sender,
                (
                    SELECT COUNT(*)
                    FROM reblooms r2
                    WHERE r2.bloom_id = b.id
                ) AS rebloom_count
            FROM reblooms r
            JOIN users ru ON ru.id = r.sender_id
            JOIN blooms b ON b.id = r.bloom_id
            JOIN users ou ON ou.id = b.sender_id
            WHERE r.id = %(bloom_id)s
            """,
            {"bloom_id": bloom_id},
        )

        row = cur.fetchone()

        if row is None:
            return None

        (
            bloom_id,
            sender_username,
            content,
            timestamp,
            original_bloom_id,
            original_sender,
            rebloom_count,
        ) = row

        return Bloom(
            id=bloom_id,
            sender=sender_username,
            content=content,
            sent_timestamp=timestamp,
            original_bloom_id=original_bloom_id,
            original_sender=original_sender,
            rebloom_count=rebloom_count,
        )


def get_blooms_with_hashtag(
    hashtag_without_leading_hash: str, *, limit: int = None
) -> List[Bloom]:
    kwargs = {
        "hashtag_without_leading_hash": hashtag_without_leading_hash,
    }

    limit_clause = make_limit_clause(limit, kwargs)

    with db_cursor() as cur:
        cur.execute(
            f"""
            WITH all_blooms AS (
                SELECT
                    b.id,
                    u.username AS sender_username,
                    b.content,
                    b.send_timestamp,
                    NULL::BIGINT AS original_bloom_id,
                    NULL::VARCHAR AS original_sender,
                    (
                        SELECT COUNT(*)
                        FROM reblooms r
                        WHERE r.bloom_id = b.id
                    ) AS rebloom_count,
                    b.id AS source_bloom_id
                FROM blooms b
                JOIN users u ON u.id = b.sender_id

                UNION ALL

                SELECT
                    r.id,
                    ru.username AS sender_username,
                    b.content,
                    r.send_timestamp,
                    r.bloom_id AS original_bloom_id,
                    ou.username AS original_sender,
                    (
                        SELECT COUNT(*)
                        FROM reblooms r2
                        WHERE r2.bloom_id = b.id
                    ) AS rebloom_count,
                    b.id AS source_bloom_id
                FROM reblooms r
                JOIN users ru ON ru.id = r.sender_id
                JOIN blooms b ON b.id = r.bloom_id
                JOIN users ou ON ou.id = b.sender_id
            )
            SELECT
                ab.id,
                ab.sender_username,
                ab.content,
                ab.send_timestamp,
                ab.original_bloom_id,
                ab.original_sender,
                ab.rebloom_count
            FROM all_blooms ab
            JOIN hashtags h ON h.bloom_id = ab.source_bloom_id
            WHERE h.hashtag = %(hashtag_without_leading_hash)s
            ORDER BY ab.send_timestamp DESC
            {limit_clause}
            """,
            kwargs,
        )

        rows = cur.fetchall()
        blooms = []

        for row in rows:
            (
                bloom_id,
                sender_username,
                content,
                timestamp,
                original_bloom_id,
                original_sender,
                rebloom_count,
            ) = row

            blooms.append(
                Bloom(
                    id=bloom_id,
                    sender=sender_username,
                    content=content,
                    sent_timestamp=timestamp,
                    original_bloom_id=original_bloom_id,
                    original_sender=original_sender,
                    rebloom_count=rebloom_count,
                )
            )

    return blooms


def make_limit_clause(limit: Optional[int], kwargs: Dict[Any, Any]) -> str:
    if limit is not None:
        limit_clause = "LIMIT %(limit)s"
        kwargs["limit"] = limit
    else:
        limit_clause = ""
    return limit_clause
