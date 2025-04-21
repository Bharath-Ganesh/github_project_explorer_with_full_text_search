import pandas as pd
import re

# Load your Excel file
df = pd.read_excel("your_file.xlsx")  # Replace with your actual file path

# Function to replace hyphen and capitalize the next word
def replace_and_capitalize(text):
    if pd.isna(text):
        return ""
    # Replace ' - word' with '. Word'
    return re.sub(r'\s*-\s*(\w+)', lambda m: '. ' + m.group(1).capitalize(), text)

# Apply to the relevant column
df["Customer Statement"] = df["Customer Statement"].apply(replace_and_capitalize)

# Save the result
df.to_excel("cleaned_output.xlsx", index=False)