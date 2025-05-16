from functools import reduce


# https://stackoverflow.com/questions/25833613/safe-method-to-get-value-of-nested-dictionary
def deep_get(dictionary, keys, default=None):
    return reduce(
        lambda d, key: d.get(key, default) if isinstance(d, dict) else default,
        keys.split("."),
        dictionary,
    )
