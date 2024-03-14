file_path = '/Users/yujinlee/Downloads/leaderboard.test.data.20130318_20.txt'

# Read the file and extract the first attribute from each line
with open(file_path, 'r') as file:
    lines = file.readlines()

count = 0

for line in lines:
    # Assuming the input line is tab-delimited
    columns = line.strip().split('\t')
    print(len(columns))

    if len(columns) > 23:  # For the first season, there are 24 columns
        adCreativeId = columns[18]  # 19th column
        width = columns[13]  # 14th column
        height = columns[14]  # 15th column
        clicks = columns[22]  # 23rd column
        conversions = columns[23]  # 24th column

        # Creating a composite value of width, height, clicks, and conversions
        compositeValue = f"{width}\t{height}\t{clicks}\t{conversions}"

        # Writing out the adCreativeId as the key and the composite values as the value
        print(f"{adCreativeId}\t{compositeValue}")
    
    count += 1
    if count > 5: break

# first_attributes = [line.split('\t')[18] for line in lines]

# # Find duplicates
# seen = set()
# duplicates = set()

# for attribute in first_attributes:
#     if attribute in seen:
#         duplicates.add(attribute)
#     else:
#         seen.add(attribute)

# # Print the duplicates and their count
# print("Duplicates:", duplicates)
# print("Number of duplicates:", len(duplicates))


