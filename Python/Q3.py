# Character Frequency Counter: Create a function to count the occurrences of a specific character in a string. Example: 'mississippi', 's' should return 3 .

def frequency_counter(user_str,user_char):
    count = 0
    for char in user_str:
        if user_char == char:
            count += 1
    
    return count

input_str = input("Enter the String :")
input_char = input("Enter the Character :")
result_count = frequency_counter(input_str,input_char) 
print(result_count)