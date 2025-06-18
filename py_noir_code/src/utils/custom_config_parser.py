import configparser


class CustomConfigParser(configparser.ConfigParser):
    def get(self, section, option, *, raw=False, vars=None, fallback=None):
        value = super().get(section, option, raw=raw, vars=vars, fallback=fallback)
        if value == "None":
            return None
        return value