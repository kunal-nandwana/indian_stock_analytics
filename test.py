import os

test_dir = "/Users/kunal.nandwana/test_data"
try:
    print("Listing files in:", test_dir)
    print(os.listdir(test_dir))
except Exception as e:
    print("Error:", e)