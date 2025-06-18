#Remove duplicate executions according to path when it happens sometimes
def deduplicate_executions(executions, dedup_key_path: str):
    def get_nested_value(obj, path: str):
        parts = path.replace(']', '').split('.')
        result = obj
        for part in parts:
            if '[' in part:
                key, index = part.split('[')
                result = result[key][int(index)]
            else:
                result = result[part]
        return tuple(result)  # convert to tuple for set hashing

    seen = set()
    unique = []

    for exec in executions:
        try:
            key = get_nested_value(exec, dedup_key_path)
            if key not in seen:
                seen.add(key)
                unique.append(exec)
        except (KeyError, IndexError, TypeError):
            # Optionally log or handle badly formed entries
            pass

    return unique