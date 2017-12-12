"""def count_words(msg):
    n = len(msg.split())
    print ("This sent has {0}".format(n))
msg = "hi good mg. how are you"
count_words(msg)

msg1 = "cool i am great"
count_words(msg1)"""

#list comprehension
dataset = ["hi good mng.how are you","cool i am great"]
n = [len(character.split()) for character in dataset]
print n

print time.time()



