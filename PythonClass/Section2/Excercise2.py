findTheType = input("Enter a no:")
print type(findTheType)
if type(findTheType) == "":
    print "it is string"
elif type(findTheType) == type(1):
    print "it is an interger"
elif type(findTheType) == type(1.0):
    print "it is a floating point"
else:
    print "it is a other than integer, floating point and string"