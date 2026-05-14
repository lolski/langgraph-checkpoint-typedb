"""Pure TypeQL query string builders.

Each function returns a TypeQL query as a string. The functions are deliberately
free of TypeDB driver imports so they can be unit-tested without a server.
"""

from __future__ import annotations

from langgraph_checkpoint_typedb.serialization import escape_tql_string

# ---------------------------------------------------------------- checkpoint


_CHECKPOINT_FETCH_FIELDS = (
    "    has thread_id $tid,\n"
    "    has checkpoint_ns $ns,\n"
    "    has checkpoint_id $cid,\n"
    "    has parent_checkpoint_id $pid,\n"
    "    has cp_type $ct,\n"
    "    has cp_blob $cb,\n"
    "    has md_type $mt,\n"
    "    has md_blob $mb,\n"
    "    has run_id $rid;\n"
    'fetch { "tid": $tid, "ns": $ns, "cid": $cid, "pid": $pid,'
    ' "ct": $ct, "cb": $cb, "mt": $mt, "mb": $mb, "rid": $rid };'
)


def fetch_checkpoint_by_path(cp_path: str) -> str:
    p = escape_tql_string(cp_path)
    return (
        "match\n"
        f'  $c isa checkpoint, has cp_path "{p}",\n'
        + _CHECKPOINT_FETCH_FIELDS
    )


def fetch_latest_checkpoint(thread_id: str, checkpoint_ns: str) -> str:
    tid = escape_tql_string(thread_id)
    ns = escape_tql_string(checkpoint_ns)
    return (
        "match\n"
        f'  $c isa checkpoint, has thread_id "{tid}", has checkpoint_ns "{ns}",\n'
        + _CHECKPOINT_FETCH_FIELDS
    )


def list_checkpoints(
    *,
    thread_id: str | None,
    checkpoint_ns: str | None,
) -> str:
    constraints: list[str] = []
    if thread_id is not None:
        constraints.append(f'has thread_id "{escape_tql_string(thread_id)}"')
    if checkpoint_ns is not None:
        constraints.append(f'has checkpoint_ns "{escape_tql_string(checkpoint_ns)}"')
    head = "$c isa checkpoint"
    if constraints:
        head = head + ", " + ", ".join(constraints)
    return (
        "match\n"
        f"  {head},\n"
        + _CHECKPOINT_FETCH_FIELDS
    )


def delete_checkpoint_by_path(cp_path: str) -> str:
    p = escape_tql_string(cp_path)
    return (
        "match\n"
        f'  $c isa checkpoint, has cp_path "{p}";\n'
        "delete $c;"
    )


def insert_checkpoint(
    *,
    cp_path: str,
    thread_id: str,
    checkpoint_ns: str,
    checkpoint_id: str,
    parent_checkpoint_id: str,
    cp_type: str,
    cp_blob: str,
    md_type: str,
    md_blob: str,
    run_id: str,
) -> str:
    return (
        "insert\n"
        "  $c isa checkpoint,\n"
        f'    has cp_path "{escape_tql_string(cp_path)}",\n'
        f'    has thread_id "{escape_tql_string(thread_id)}",\n'
        f'    has checkpoint_ns "{escape_tql_string(checkpoint_ns)}",\n'
        f'    has checkpoint_id "{escape_tql_string(checkpoint_id)}",\n'
        f'    has parent_checkpoint_id "{escape_tql_string(parent_checkpoint_id)}",\n'
        f'    has cp_type "{escape_tql_string(cp_type)}",\n'
        f'    has cp_blob "{escape_tql_string(cp_blob)}",\n'
        f'    has md_type "{escape_tql_string(md_type)}",\n'
        f'    has md_blob "{escape_tql_string(md_blob)}",\n'
        f'    has run_id "{escape_tql_string(run_id)}";'
    )


# ---------------------------------------------------------------- blob


def fetch_blob_by_path(blob_path: str) -> str:
    p = escape_tql_string(blob_path)
    return (
        "match\n"
        f'  $b isa checkpoint_blob, has blob_path "{p}",\n'
        "    has channel $ch,\n"
        "    has version $v,\n"
        "    has blob_type $bt,\n"
        "    has blob_value $bv;\n"
        'fetch { "ch": $ch, "v": $v, "bt": $bt, "bv": $bv };'
    )


def fetch_blobs_for_thread_ns(thread_id: str, checkpoint_ns: str) -> str:
    tid = escape_tql_string(thread_id)
    ns = escape_tql_string(checkpoint_ns)
    return (
        "match\n"
        f'  $b isa checkpoint_blob, has thread_id "{tid}", has checkpoint_ns "{ns}",\n'
        "    has channel $ch,\n"
        "    has version $v,\n"
        "    has blob_type $bt,\n"
        "    has blob_value $bv;\n"
        'fetch { "ch": $ch, "v": $v, "bt": $bt, "bv": $bv };'
    )


def insert_blob(
    *,
    blob_path: str,
    thread_id: str,
    checkpoint_ns: str,
    channel: str,
    version: str,
    blob_type: str,
    blob_value: str,
) -> str:
    return (
        "insert\n"
        "  $b isa checkpoint_blob,\n"
        f'    has blob_path "{escape_tql_string(blob_path)}",\n'
        f'    has thread_id "{escape_tql_string(thread_id)}",\n'
        f'    has checkpoint_ns "{escape_tql_string(checkpoint_ns)}",\n'
        f'    has channel "{escape_tql_string(channel)}",\n'
        f'    has version "{escape_tql_string(version)}",\n'
        f'    has blob_type "{escape_tql_string(blob_type)}",\n'
        f'    has blob_value "{escape_tql_string(blob_value)}";'
    )


def delete_blob_by_path(blob_path: str) -> str:
    p = escape_tql_string(blob_path)
    return (
        "match\n"
        f'  $b isa checkpoint_blob, has blob_path "{p}";\n'
        "delete $b;"
    )


def delete_blobs_for_thread(thread_id: str) -> str:
    tid = escape_tql_string(thread_id)
    return (
        "match\n"
        f'  $b isa checkpoint_blob, has thread_id "{tid}";\n'
        "delete $b;"
    )


# ---------------------------------------------------------------- write


def fetch_writes_for_checkpoint(
    thread_id: str, checkpoint_ns: str, checkpoint_id: str
) -> str:
    tid = escape_tql_string(thread_id)
    ns = escape_tql_string(checkpoint_ns)
    cid = escape_tql_string(checkpoint_id)
    return (
        "match\n"
        f'  $w isa checkpoint_write, has thread_id "{tid}",'
        f' has checkpoint_ns "{ns}", has checkpoint_id "{cid}",\n'
        "    has task_id $tk,\n"
        "    has task_path $tp,\n"
        "    has write_idx $i,\n"
        "    has channel $ch,\n"
        "    has write_type $wt,\n"
        "    has write_blob $wb;\n"
        'fetch { "tk": $tk, "tp": $tp, "i": $i, "ch": $ch, "wt": $wt, "wb": $wb };'
    )


def insert_write(
    *,
    write_path: str,
    thread_id: str,
    checkpoint_ns: str,
    checkpoint_id: str,
    task_id: str,
    task_path: str,
    write_idx: int,
    channel: str,
    write_type: str,
    write_blob: str,
) -> str:
    return (
        "insert\n"
        "  $w isa checkpoint_write,\n"
        f'    has write_path "{escape_tql_string(write_path)}",\n'
        f'    has thread_id "{escape_tql_string(thread_id)}",\n'
        f'    has checkpoint_ns "{escape_tql_string(checkpoint_ns)}",\n'
        f'    has checkpoint_id "{escape_tql_string(checkpoint_id)}",\n'
        f'    has task_id "{escape_tql_string(task_id)}",\n'
        f'    has task_path "{escape_tql_string(task_path)}",\n'
        f"    has write_idx {write_idx},\n"
        f'    has channel "{escape_tql_string(channel)}",\n'
        f'    has write_type "{escape_tql_string(write_type)}",\n'
        f'    has write_blob "{escape_tql_string(write_blob)}";'
    )


def delete_write_by_path(write_path: str) -> str:
    p = escape_tql_string(write_path)
    return (
        "match\n"
        f'  $w isa checkpoint_write, has write_path "{p}";\n'
        "delete $w;"
    )


def delete_writes_for_thread(thread_id: str) -> str:
    tid = escape_tql_string(thread_id)
    return (
        "match\n"
        f'  $w isa checkpoint_write, has thread_id "{tid}";\n'
        "delete $w;"
    )


def delete_writes_for_checkpoint(
    thread_id: str, checkpoint_ns: str, checkpoint_id: str
) -> str:
    tid = escape_tql_string(thread_id)
    ns = escape_tql_string(checkpoint_ns)
    cid = escape_tql_string(checkpoint_id)
    return (
        "match\n"
        f'  $w isa checkpoint_write, has thread_id "{tid}",'
        f' has checkpoint_ns "{ns}", has checkpoint_id "{cid}";\n'
        "delete $w;"
    )


# ---------------------------------------------------------------- thread-wide


def delete_checkpoints_for_thread(thread_id: str) -> str:
    tid = escape_tql_string(thread_id)
    return (
        "match\n"
        f'  $c isa checkpoint, has thread_id "{tid}";\n'
        "delete $c;"
    )


def fetch_checkpoint_ids_for_thread(thread_id: str) -> str:
    tid = escape_tql_string(thread_id)
    return (
        "match\n"
        f'  $c isa checkpoint, has thread_id "{tid}", has checkpoint_ns $ns,'
        " has checkpoint_id $cid;\n"
        'fetch { "ns": $ns, "cid": $cid };'
    )
