import json

import threading

from itertools import chain, islice


def convert_country_code(country_code):
    code_dict = {
        'AN': ['BQ', 'CW', 'SX'],  # The Netherlands Antilles was divided into
        # Bonaire, Saint Eustatius and Saba (BQ)
        # Curaçao (CW)
        # and Sint Maarten (SX)
        'AQ': 'AQ',  # Antarctica is not even on the map
        'BU': 'MM',  # Burma is now Myanmar
        'CS': ['RS', 'SK'],  # Czechoslovakia was divided into Czechia (CZ), and Slovakia (SK)
        'SU': ['AM', 'AZ', 'EE', 'GE', 'KZ', 'KG', 'LV', 'LT', 'MD', 'RU', 'TJ', 'TM', 'UZ'],  # USSR was divided into:
        # Armenia (AM),
        # Azerbaijan (AZ),
        # Estonia (EE),
        # Georgia (GE),
        # Kazakstan (KZ),
        # Kyrgyzstan (KG),
        # Latvia (LV),
        # Lithuania (LT),
        # Republic of Moldova (MD),
        # Russian Federation (RU),
        # Tajikistan (TJ),
        # Turkmenistan (TM),
        # Uzbekistan (UZ).
        'TP': 'TL',  # Name changed from East Timor (TP) to Timor-Leste (TL)
        'UM': ['UM-DQ', 'UM-FQ', 'UM-HQ', 'UM-JQ', 'UM-MQ', 'UM-WQ'],  # United States Minor Outlying Islands is
        # Jarvis Island   (UM-DQ)
        # Baker Island    (UM-FQ)
        # Howland Island  (UM-HQ)
        # Johnston Atoll  (UM-JQ)
        # Midway Islands  (UM-MQ)
        # Wake Island     (UM-WQ)
        'XC': 'IC',  # Czechoslovakia was divided into Czechia (CZ), and Slovakia (SK)
        'XG': 'DE',  # East Germany is now germany (DE)
        'XI': 'IM',  # Northern Ireland is kind of Isle of man
        'YU': ['BA', 'HR', 'MK', 'CS', 'SI'],  # Former Yugoslavia was divided into
        # Bosnia and Herzegovina (BA),
        # Croatia (HR),
        # The former Yugoslav Republic of Macedonia (MK),
        # Serbia and Montenegro (CS),
        # Slovenia (SI)
        'ZR': 'CD'  # Name changed from Zaire to the Democratic Republic of the Congo (CD)
    }

    for old_code, new_codes in code_dict.items():
        if country_code in new_codes:
            return f"'{old_code}', '{country_code}'"
    return f"'{country_code}'"


def chunks(iterable, size=100):
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


def start_background_process(target, thread_name, log_id):
    if thread_name not in [thread.name for thread in threading.enumerate()]:
        thread = threading.Thread(target=target, name=thread_name)
        thread.daemon = True
        thread.start()
        return json.dumps({"Message": f"Starting to process {log_id}"})
    else:
        return json.dumps({"Message": f"{log_id} process already started"})
