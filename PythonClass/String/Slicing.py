#Question came for dynamic range
#how its moving towards opposite direction

name = "Surendra Saxena"
print name[:5]
print name[-1:5:-1]
print name[::-1]

msg = "hi good morning how are you"
new_Msg_Practice = msg[-7:-4]
msg_Practice = msg[-5:-8:-1]
print new_Msg_Practice #need to ask
print msg_Practice #need to ask

new_msg = "......+++.hi Surendra how are you. how you doing............"
new_Msg_Practice1 = new_msg.count("how")
#one string might be multiple special charater. how to play around with that.
new_Msg_Practice2 = new_msg.strip(".+")



print new_Msg_Practice1
print new_Msg_Practice2
