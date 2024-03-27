def escape_prometheus(text: str) -> str:
    return text.replace("\\", "\\\\").replace("\"", "\\\"")
