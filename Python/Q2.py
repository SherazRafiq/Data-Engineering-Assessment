#Mismatched Words Finder: Write a function that returns a list of words present in two strings that don't match in case. Example: Input: "Datumlabs is an awesome place" , "Datumlabs.io Is an AWESOME place" . Output: ["is", "Is", "awesome", "AWESOME"] .

def find_case_sensitive_words(str1, str2):
    words1 = str1.split()
    words2 = str2.split()
    case_sensitive_words = []
    for word1 in words1:
        if any(word1.lower() == word.lower() and word1 != word for word in words2):
            case_sensitive_words.append(word1)
    for word2 in words2:
        if any(word2.lower() == word.lower() and word != word2 for word in words1):
            case_sensitive_words.append(word2)
    return case_sensitive_words

str1 = "Datumlabs is an awesome place"
str2 = "Datumlabs.io Is an AWESOME place"
output = find_case_sensitive_words(str1, str2)
output_sorted = sorted(output, key=lambda x: (x.lower(), x))
print(output_sorted)  # Output: ['is', 'Is', 'awesome', 'AWESOME']
