#why its not printing palindrome only printing not a palindrom
#why you put list there
#!usr/bin/python
my_inputdata = input("Enter a String: ")
my_inputdata = my_inputdata.lower()
rev_my_str = my_inputdata[::-1]
if my_inputdata == rev_my_str:
    print "String is palindrom"
else:
    print "String is not palindrom"
