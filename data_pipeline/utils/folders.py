import os

def create_custom_temp_dir():
    temp_dir = "temporary"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    return temp_dir

