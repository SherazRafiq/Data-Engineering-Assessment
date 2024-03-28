#Fill None Values: Given a list, replace None values with the previous non- None value. If consecutive None s occur, fill each with the last non- None value. Example: [1, None, 1, 2, None] becomes [1, 1, 1, 2, 2] .


def fill_none(list_nums):
    prev_non_none = None
    for i in range(len(list_nums)):
        if list_nums[i] is not None:
            prev_non_none = list_nums[i]
        else:
            list_nums[i] = prev_non_none
        
    return list_nums

nums = [1,None,None,None,None,5]

result_list = fill_none(nums)
print(result_list)
