"""K5 indexing caps in UTF-8 bytes; qmd tool content stays metadata only."""

CAPS = {
    'authored': 65536,
    'attachment_text': 65536,
    'tool_output': 2048,
    'header': 65536,
    'quoted': 0,
    'boilerplate': 0,
    'binary': 0,
    'summary': 0,
}
KIND_WEIGHT = {'authored': 1.0, 'session': 1.0, 'tool_output': 0.5}


def text(doc):
    out = []
    used = 0
    cap = 262144 if doc.get('source') == 'files' else 65536
    for part in doc.get('parts', []):
        if part.get('tool') == 'qmd':
            continue
        part_cap = (
            cap
            if doc.get('source') == 'files' and part['kind'] in ('authored', 'attachment_text')
            else CAPS.get(part['kind'], 0)
        )
        n = min(part_cap, cap - used)
        value = part.get('text', '').encode()[:n].decode('utf-8', 'ignore')
        if value:
            out.append(value)
            used += len(value.encode())
        if used >= cap:
            break
    return '\n'.join(out)
