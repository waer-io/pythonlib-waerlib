import os
import waerlib

if __name__ == '__main__':
    df = waerlib.read('0a643aa4-9a51-4676-a524-2582adbefd50','2021-12-01','2023-12-31', [
        'activity|data|heart_rate_data|summary|max_hr_bpm','daily|data|calories_data|BMR_calories'
    ], 'parsed')
