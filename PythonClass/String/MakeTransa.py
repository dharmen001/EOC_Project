from string import  maketrans
msg = "this is my example...wow!!"
previous_data = "aeiou"
new_data = "12345"
trans = maketrans(previous_data,new_data)
new_trans = msg.translate(trans)
print msg
print new_trans