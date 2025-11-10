# Perfect practice challenge: This one combines file handling, loops, and functions.

def get_even_number_from_file(filename):
    even_number = []
    try:
    # Open the file and read numbers
        with open(filename, 'r') as file:
            for line in file:
                try:
                    number = int(line.strip())
                    if number % 2 == 0:
                        even_number.append(number)
                except ValueError:
                    # Handle the case where the file does not exist
                    print(f"Skipping invalid line: {line.strip()}")
        return even_number
    except FileNotFoundError:
        print(f"The file {filename} does not exist.")
        return []
    
    
# Example usage
even_number = get_even_number_from_file('numbers.txt')
print(even_number)
            
        