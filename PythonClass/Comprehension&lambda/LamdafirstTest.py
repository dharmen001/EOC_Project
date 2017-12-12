name = "Deepak Kumar"
def manipulation():
    f_name = name.split()[0]
    l_name = name.split()[1]
    return f_name,l_name

new_manipulation = lambda name: name.split()[0],name.split()[1]
for val in new_manipulation:
    print val
