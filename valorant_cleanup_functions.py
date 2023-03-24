import pandas as pd

def add_role(x, numeric=True, string=False):
        """
        Changes the categorical cut data into numeric.
        Hard coded to specify that premium is the best
        :x: the passed value from apply
        :return: differently numerical values based on input
        Example use: Y['agents'] = Y['agents'].apply(imported_module_name.add_role).values
        """
        if numeric:
            if x in ["Sova", "Fade", "Kayo", "Gekko", "Breach"]: return 0 # Initiator
            if x in ["Reyna", "Jett", "Raze", "Neon", "Yoru", "Phoenix"]: return 1 # Duelist
            if x in ["Omen", "Astra", "Viper", "Harbor", "Brimstone"]: return 2 # Controller
            if x in ["Killjoy", "Cypher", "Chamber", "Sage"]: return 3 # Sentinel
        # if numeric:
        #     if x in ["Reyna", "Jett", "Raze", "Neon", "Yoru", "Phoenix"]: return 0 # Duelist
        #     else:
        #         return 1 # something else
        #     # if x in : return 1 # Duelist
        #     # if x in ["Omen", "Astra", "Viper", "Harbor", "Brimstone"]: return 2 # Controller
        #     # if x in ["Killjoy", "Cypher", "Chamber", "Sage"]: return 3 # Sentinel

def add_custom_agent_category(x, y: list[str], numeric=True, string=False):
    if numeric:
        if x in y: return 0
        else: return 1
