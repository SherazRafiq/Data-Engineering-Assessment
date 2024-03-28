# Nth Largest Value Key Finder: Write a function to find the key of the nth largest value in a dictionary. Example: For {a: 1, b: 2, c: 100, d: 30} , and n = 2 , return 'd' .

def nth_largest_key(dictionary, n):
    sorted_keys = sorted(dictionary, key=dictionary.get, reverse=True)
    return sorted_keys[n - 1] if n <= len(sorted_keys) else None

input_dict = {'a': 1, 'b': 2, 'c': 100, 'd': 30}
n = 2
result = nth_largest_key(input_dict, n)
print(result)
