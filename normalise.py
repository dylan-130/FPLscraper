import json

def main():
    input_file = "decoded_data.json"
    output_file = "normalised_data.json"
    
    # 1. Read the JSON data from decoded_data.json
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # 2. Normalize the 'Key' for each record
    for item in data:
        original_key = item.get("Key", "")
        # strip leading/trailing whitespace and make lowercase
        normalized_key = original_key.strip().lower()
        item["Key"] = normalized_key
    
    # 3. Write the updated data to normalised_data.json
    with open(output_file, "w", encoding="utf-8") as f:
        # Use ensure_ascii=False to preserve any unicode characters (e.g. emojis)
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    print(f"Normalization complete! Output written to '{output_file}'.")

if __name__ == "__main__":
    main()
