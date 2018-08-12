text = 'Temperature: 24.145 oC'

temp = text.rstrip().split('Temperature:')[1]
temp = temp.split('oC')[0]
print (temp)

y = []
y = (text.split(' '))
print (float(y[1]))