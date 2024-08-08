import os
from typing import List, Dict, Any

def editXMLandSave(values_to_change: Dict[str, Dict[str, Any]], 
                   #{"file_to_change": {"source": "source_file", "destination": "destination_file",
                   #                    "parameters": {"key": "value"}
                   #                   }
                   # }
                   **kwargs
                   ):
    """
    Edit the XML file with the design parameters and save it to a new file.
    """
    # Let us loop values to change
    for file_to_change, values in values_to_change.items():
        print (f"Editing file {file_to_change}")
        # Read the source file
        source = values.get("source")
        destination = values.get("destination")
        parameters = values.get("parameters")
        # check if the source file exists
        if not os.path.exists(source):
            raise FileNotFoundError(f"File {source} does not exist.")
        # Read the source file
        with open(source, "r") as f:
            content = f.read()
            # Loop the parameters and replace the values
            for key, value in parameters.items():
                content = content.replace(key, value)
        # Save the content to the destination file
        with open(destination, "w") as f:
            f.write(content)

if __name__ == "__main__":
    pass
