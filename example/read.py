import os
import waerlib

if __name__ == '__main__':
    df = waerlib.read('24b38dfc-4f46-46da-a64d-1b429035c514','2021-12-01','2023-12-31', [
        'activity|data|heart_rate_data|summary|max_hr_bpm','daily|data|calories_data|BMR_calories'
    ], 'parsed')

