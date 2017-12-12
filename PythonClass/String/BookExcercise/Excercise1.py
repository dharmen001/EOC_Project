print "Excercise1"
name_Excercise1 = input("Enter a string to reverse:").upper()
new_name_Excercise1 = name_Excercise1[::-1]
#rev_Excercise1 = reversed(new_name_Excercise1)#object
print (new_name_Excercise1)
#print rev_Excercise1

print "ExcerCise2"
name_Excercise2 = input("Enter a string to check palindrome:")
new_name_Excercise2 = name_Excercise2[::-1].upper()
if name_Excercise2 == new_name_Excercise2:
    print "String is palindrom"
else:
    print "String is not palindrom"

print "ExcerCise3"
name_Excercise3 = input("Enter a string for split:")
#how to use upper and lower with split
new_name_Excercise3 = name_Excercise3.split("s")
print (new_name_Excercise3)

print "ExcerCise4"
name_Excercise4 = input("Enter a string for split:")
#how to use upper and lower with split
new_name_Excercise4 = name_Excercise3.partition("s")#how partition is working
print (new_name_Excercise4)

print "ExcerCise5"
#??

print "ExcerCise6"
print 'i like gita\'s pink colour dress'

print "ExcerCise7"
str = 'Honesty is the best policy'
print str.replace("o","*")






print "ExcerCise8"
str = "Hello World"
print str.istitle()

print "ExcerCise9"
str = "Group Discussion"
print str.lstrip("Gro")

print "ExcerCise10"
str = input("Enter a String to print alternate series")
print str[0:len(str):2]

print "ExcerCise11"
str = input("Enter a String 'python' " )
print str.replace("y","")

print "ExcerCise12a"
str = "Global Warming"
print str[-4:len(str)] #why -1 is not working

print "ExcerCise12b"
str = "Global Warming"
print str[4:8] #??

print "ExcerCise12c"
str = "Global Warming"
print str.isalnum()

print "ExcerCise12d"
str = "Global Warming"
print str[:len(str)-4]

print "ExcerCise12e"
str = "Global Warming"
print str[4-len(str):]

print "ExcerCise12f"
str = "Global Warming"
str_split = str.split()
str_split_new = str_split[1]
print str_split_new[0:2]

print "ExcerCise12g"
str = "Global Warming"
print str.swapcase()

print "ExcerCise12h"
str = "Global Warming"
print str.istitle()

print "ExcerCise12h"
str = "Global Warming"
print str.replace("a","*")

print "ExcerCise13"
#??

print "Excercise14"
#??

print "Excercise15"

str = "Hello World"
str[5] = "p"







