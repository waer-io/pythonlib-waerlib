import waerlib

if __name__ == '__main__':
    data = {'user':{'user_id': '123-321'}, 'data':[{'val1':1, 'val2':2},{'val1':3,'val2':4}]}
    df = waerlib.store_raw(data)
    
