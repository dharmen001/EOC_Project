name = "hi @gauravgmail.com"
name1 = name.split("@")
name2 = name.partition("@")
name3 = name.split("@")[-1]
name4 = name.partition("@")[0]
print name1
print name2
print name3
print name4

Concat_the_values = " ".join([name3,name4])
print Concat_the_values