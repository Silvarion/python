"""
Consider a list (list = []). You can perform the following commands:

insert i e: Insert integer  at position .
print: Print the list.
remove e: Delete the first occurrence of integer .
append e: Insert integer  at the end of the list.
sort: Sort the list.
pop: Pop the last element from the list.
reverse: Reverse the list.
Initialize your list and read in the value of  followed by  lines of commands where each command will be of the  types listed above. Iterate through each command in order and perform the corresponding operation on your list.

Constraints

The elements added to the list must be integers.
Output Format

For each command of type print, print the list on a new line.

Sample Input 0

12
insert 0 5
insert 1 10
insert 0 6
print
remove 6
append 9
append 1
sort
print
pop
reverse
print
Sample Output 0

[6, 5, 10]
[1, 5, 9, 10]
[9, 5, 1]
"""
if __name__ == '__main__':
    N = int(input())
    lst = []
    for _ in range(N):
        line = str(input()).split(" ")
        if line[0] == "append":
            lst.append(int(line[1]))
        elif line[0] == "insert":
            lst.insert(int(line[1]),int(line[2]))
        elif line[0] == "pop":
            lst.pop()
        elif line[0] == "print":
            print(lst)
        elif line[0] == "remove":
            lst.remove(int(line[1]))
        elif line[0] == "reverse":
            lst.reverse()
        elif line[0] == "sort":
            lst.sort()
