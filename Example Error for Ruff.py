def greet(name):
    print("Hello, " + name)


greet("Alice")


def add_numbers(a, b):
    return a + b


result = add_numbers(5, "10")  # This will cause a TypeError

print("The result is: " + result)  # This will cause a TypeError

print("Hello World")  # This will trigger the debug-statements hook

print("Goodbye World")  # This will trigger the debug-statements hook
