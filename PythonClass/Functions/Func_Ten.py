def a(x):
    return x+1
result = a(6)
print result

def b(x):
    return x+1.0
result = b(-5.3)
print result

def c(x,y):
    return x+y
result = c(a(1),b(1))
print result

def bigger_number(x,y):
    #x = input("Enter the first number")
    #y = input("Enther the second number")
    if x>y:
        return x
    else:
        return y

result = bigger_number(input("Enter the first number"), input("Enter the second number"))
print result



