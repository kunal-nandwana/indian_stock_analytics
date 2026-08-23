# # from nselib import capital_market

# # # Use the specific index data function instead
# # from_period_nse = '18-06-2026'
# # to_period_nse = '19-06-2026'
# # data = capital_market.index_data(
# #     index="NIFTY Bank", 
# #     from_date=from_period_nse, 
# #     to_date=to_period_nse
# # )

# # print(data)


# # def factoria(n):

# #     fact=1
# #     for i in range(1,n+1):
# #         fact=fact*i

# #     return fact

# # print(factoria(5))


# # def find_the_winner(n,k):
# #     arr=[i+1 for i in range(n)]

# #     def helper(arr,start_index):
# #         if len(arr)==1:
# #             return arr[0]
        
# #         index_to_remove=(start_index+k-1)%len(arr)
# #         del arr[index_to_remove]
# #         return helper(arr,index_to_remove)
    
# #     return helper(arr,0)


# # print(find_the_winner(9,3))


# # def toh(N, fromm, to, help):
# #     count = 0
    
# #     def helper(N, fromm, to, help):
# #         nonlocal count
# #         if N == 1:
# #             print("move disk " + str(N) + " from rod " + str(fromm) + " to rod " + str(to))
# #             count += 1
# #             return

# #         helper(N - 1, fromm, help, to)  # Left Child (Phase 1)
# #         #helper(2,R1,R2,R3)
# #         print("move disk " + str(N) + " from rod " + str(fromm) + " to rod " + str(to)) # Parent Work (Phase 2)
# #         count += 1
# #         helper(N - 1, help, to, fromm)       # Right Child (Phase 3)

# #     # THE FIX: Kick off the helper, then return the final total count!
# #     helper(N, fromm, to, help)
# #     return count


# # print(toh(4,'R1','R3','R2'))




# # def rec(arr):
    
# #     total=0
# #     levels=0
# #     def helper(arr):
# #         nonlocal total
# #         nonlocal levels
# #         for i in arr:
            
# #             if(type(i)==int):
# #                 total=total+i

# #             if(type(i)==list):
# #                 helper(i)
# #                 levels=levels+1
# #         return total+levels+1
    
# #     return helper(arr)






# # arr=[1,2,3,[4,5,[1,2]],[1,2,4]]
# # print(rec(arr))

# # # [1,2]
# # # [4,5[1,2]]



# # name='kunal'

# # print(list(name))

# # def reverse_string_index(s, index):
# #     # Base Case: When the index reaches the last character
# #     if index == len(s) - 1:
# #         return s[index] ## t
    

# #     return reverse_string_index(s, index + 1) + s[index] 

# # # --- Test the Code ---
# # input_str = "cat"
# # # Start at index 0 (the beginning of the string)
# # output_str = reverse_string_index(input_str, 0)

# # print("Original:", input_str)
# # print("Reversed:", output_str)

# # # return s[2]

# # reverse_string_index("cat",2)+"a" - "t"+"a"
# # reverse_string_index("cat",1)+"c"
# # reverse_string_index("cat",0)



# # def factorial(n):
# #     if n == 0 or n == 1:
# #         return 1
# #     else:
# # 
# # 
# #         return n * factorial(n - 1)




# # def countdown(n):
# #     if n==0:
# #         return "BlastOff!"
# #     print(n)
# #     return countdown(n-1)


# # print(countdown(5))

# # # countdown(5)
# # # countdown(4)
# # # countdown(3)
# # # countdown(2)
# # # countdown(1)
# # # countdown(0)


# # def sum(n):
# #     sum=0
# #     def helper(n):
# #         nonlocal sum
# #         if n==0:
# #             return sum
# #         sum=n+sum
# #         return helper(n-1)
# #     return helper(n)

# # print(sum(4))

# # sum(0) - 0 
# # sum(1) - sum=0+1=1
# # sum(2) - sum=1+2=3
# # sum(3) - sum=3+3=6
# # sum(4) - sum=4+6=10

# # power(2,0) - 1
# # base*power(2,1) 2
# # base*power(2,2) - 4
# # power(2,3) 2*3=8


# # def powers(n,k):
# #     if k==0:
# #         return 1
# #     return n*powers(n,k-1)

# # return 2*powers(2,0) - 1
# # return 2*powers(2,1)
# # return 2*powers(2,2)

# # print(powers(2,3))

# # def count_char(s,letter):
# #     sum=0
# #     for i in s:
# #         if i==letter:
# #             sum+=1
# #     return sum

# # print(count_char("apple","e"))


# # def count_rec(s,letter):
# #     if s=="":
# #         return 1

# #     return count_rec(s,letter)



# # def power_sum(array,power=1):
# #     sum=0
# #     for i in array:
# #         if type(i)==list:
# #             sum+=power_sum(i,power+1)
# #         else:
# #             sum+=i

# #     return sum**power

# # arrr=[1,2,[3,4],[[2]]]



# # def count_flat(array):
# #     count=0
# #     def helper(array):
# #         nonlocal count
# #         for i in array:
# #             if type(i)==list:
# #                helper(i)
# #             else:
# #                 count+=1
# #         return count
# #     return helper(array)

# # a=[1, 2, [3, 4], [[2]]]
# # print(count_flat(a))


# # def sum_el(arr):
# #     if len(arr)==0:
# #         return 0
    
# #     return arr[0]+sum_el(arr[1:])


# # [1,2,3,4,5]

# # 1+sum_el([2,3,4,5])
# # 2+sum_el([3,4,5])
# # 3+sum_el([4,5])
# # 4+sum_el([5])
# # 5+sum_el([])

# # 5+0=5
# # 4+5=9
# # 3+9=12
# # 2+12=14
# # 1+14=15


# # a = [1, [2, 3], [[4]]]

# # def flatten_nested(arr):
# #     tmp=[]
# #     def helper(arr):
# #         for i in arr:
# #             if type(i)==list:
# #                 helper(i)
# #             else:
# #                 tmp.append(i)
# #         return tmp
# #     return helper(arr)


# # print(flatten_nested(a))


# # def sum_of_digits(digit):
# #     def helper(digit):
# #         if len(digit)==1:
# #             return 0
# #         return sum+helper(digit=digit%10)
# #     helper(digit)

# # print(sum_of_digits(345))

# # 345
# # 45

# user_profile = {
#     "name": "Kunal",
#     "settings": {
#         "theme": "dark",
#         "notifications": {
#             "email": True  # <-- Find this!
#         }
#     }
# }


# def find_key(nested_dict, target_key):

#     for key,value in nested_dict.items():

#         if key==target_key:
#             return value
        
#         if type(value)==dict:
#             result=find_key(value,target_key)

#             if result is not None:
#                 return result
    

# print(find_key(user_profile,"email"))

# def permute(nums):
#     n=len(nums)
#     res=[]

#     def helper(index):
#         if index==n-1:
#             res.append(nums[:])
#         for j in range(index,n):
#             nums[index],nums[j]=nums[j],nums[index]
#             helper(index+1)
#             nums[index],nums[j]=nums[j],nums[index]
    
#     helper(0)
#     return res




# def reverse_string(str):
#     if len(str)==1:
#         return str
    
#     return str[-1] + reverse_string(str[:-1])

# print(reverse_string("kunal"))


# def check_pali(str):
#     if len(str)<=1:
#         return True
    
#     return str[0]==str[-1] and check_pali(str[1:-1])

# print(check_pali("hello"))

# ([2, 4, 2, 8, 2], 2) should return 3.

# a=[2, 4, 2, 8, 2]
# nmbr=2




# def count_occurence(arr,nmbr):
    
#     if not arr:
#         return 0
    

#     current_match = 1 if arr[0]==nmbr else 0

#     return current_match + count_occurence(arr[1:],nmbr)



# print(count_occurence(a,nmbr))

# aabbcc
# abbcc
# bbcc
# bcc
# cc
# c 


# def remove_dups(string):
#     # Base Case: Strings of length 0 or 1 cannot have duplicates
#     if len(string) <= 1:
#         return string

#     # If the first two characters match, skip the first one
#     if string[0] == string[1]:
#         return remove_dups(string[1:])
    
#     # If they don't match, keep the first one and process the rest
#     else:
#         return string[0] + remove_dups(string[1:])

# print(remove_dups("mmaannssii"))  # Output: "abc"


