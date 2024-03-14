file_path = '/Users/yujinlee/Google_Drive/NYU/Spring_2024/RBDA/project_data/imp.20131027.txt'

# Read the file and extract the first attribute from each line
with open(file_path, 'r') as file:
    lines = file.readlines()

for line in lines:
    ad_id = line.split('\t')[18]
    if ad_id == "12625":
        print(line.split('\t')[13], line.split('\t')[14])

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


