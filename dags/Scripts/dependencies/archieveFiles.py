import os
import glob

def archiveFile(source_dir, target_dir, file_pattern):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    files_to_move = glob.glob(os.path.join(source_dir, file_pattern))

    for file in files_to_move:
        file_name = os.path.basename(file)
        dest_path = os.path.join(target_dir, file_name)

        # move the file
        os.rename(file, dest_path)
        print(f"Moved {file} to {dest_path}")
    print("File movement complete...")

    
