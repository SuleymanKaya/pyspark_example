# QUESTION 1:
import json

data = [
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
]

departments = {}
for name, department, salary in data:
    if department not in departments:
        departments[department] = []
    departments[department].append({"Employee Name": name, "Salary": salary})

json_output = json.dumps(departments, indent=3)
print(json_output)
