import os
import sys
import threading
import time
from fuse import FUSE
from vcfs import VCFS


def show_menu():
    GREEN = '\033[92m'
    RESET = '\033[0m'
    print(f"\n{GREEN}" + "="*50)
    print("VCFS - Version Control File System")
    print("="*50)
    print("1. Write to file")
    print("2. Read file content")
    print("3. Read versions file")
    print("4. Rollback")
    print("5. Reverse rollback")
    print("6. Exit")
    print("="*50 + f"{RESET}")


def handle_write_to_file(mount_point, test_file):
    print("\nEnter the content you want to write to the file:")
    content = input("> ")
    
    mode = input("\nChoose write mode:\n1. Overwrite (w)\n2. Append (a)\nEnter choice (1 or 2): ")
    
    if mode == "1":
        with open(test_file, 'w') as f:
            f.write(content)
        print("Content written to file (overwrite mode).")
    elif mode == "2":
        with open(test_file, 'a') as f:
            f.write("\n" + content)
        print("Content appended to file.")
    else:
        print("Invalid choice. Defaulting to overwrite mode.")
        with open(test_file, 'w') as f:
            f.write(content)
        print("Content written to file (overwrite mode).")


def handle_read_file(test_file):
    WHITE = '\033[97m'
    RESET = '\033[0m'
    
    try:
        with open(test_file, 'r') as f:
            content = f.read()
        
        if content:
            print("\nCurrent file contents:")
            print("-" * 30)
            print(f"{WHITE}{content}{RESET}")
            print("-" * 30)
        else:
            print("\nFile is empty.")
    except FileNotFoundError:
        print("\nFile does not exist yet. Write to it first.")


def handle_read_versions(backing_dir):
    RED = '\033[91m'
    RESET = '\033[0m'
    
    safe_name = "test.txt"
    diff_file = os.path.join(backing_dir, ".vcfs_meta", f"{safe_name}.diffs")
    
    try:
        with open(diff_file, 'r') as f:
            versions_content = f.read()
        
        if versions_content:
            print("\nVersions file contents:")
            print("-" * 30)
            print(f"{RED}{versions_content}{RESET}")
            print("-" * 30)
        else:
            print("\nNo version history available.")
    except FileNotFoundError:
        print("\nNo version history file found.")


def handle_rollback(fs):
    try:
        fs.rollback("/test.txt")
    except Exception as e:
        print(f"Rollback failed: {e}")


def handle_reverse_rollback(fs):
    try:
        fs.reverse_rollback("/test.txt")
    except Exception as e:
        print(f"Reverse rollback failed: {e}")


if __name__ == "__main__":
    if os.name != "posix":
        sys.exit("ERROR: This filesystem must be run on a POSIX-compliant OS (Linux/macOS).")

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <backing_dir>")
        sys.exit(1)

    backing_dir = os.path.abspath(sys.argv[1])
    if not os.path.exists(backing_dir):
        os.makedirs(backing_dir)

    parent_dir = os.path.dirname(backing_dir)
    dir_name = os.path.basename(backing_dir)
    mount_point = os.path.join(parent_dir, f"{dir_name}_mount")
    
    # Clean up any existing mount point
    if os.path.exists(mount_point):
        print(f"Cleaning up existing mount point: {mount_point}")
        try:
            # Try to unmount if it's mounted
            os.system(f"fusermount -u {mount_point} 2>/dev/null")
            time.sleep(1)
            # Remove the directory
            os.system(f"rm -rf {mount_point}")
        except Exception as e:
            print(f"Warning: Could not clean existing mount point: {e}")
    
    os.makedirs(mount_point, exist_ok=True)

    def mount_fs():
        print(f"Mounting VCFS:\n  Backing dir: {backing_dir}\n  Mount point: {mount_point}")
        FUSE(VCFS(backing_dir), mount_point, nothreads=True, foreground=True)

    t = threading.Thread(target=mount_fs, daemon=True)
    t.start()

    time.sleep(2)

    test_file = os.path.join(mount_point, "test.txt")
    fs = VCFS(backing_dir)

    # Interactive menu loop
    while True:
        show_menu()
        choice = input("Enter your choice (1-6): ")
        
        if choice == "1":
            handle_write_to_file(mount_point, test_file)
            time.sleep(2)
        elif choice == "2":
            handle_read_file(test_file)
            time.sleep(2)
        elif choice == "3":
            handle_read_versions(backing_dir)
            time.sleep(2)
        elif choice == "4":
            handle_rollback(fs)
            time.sleep(2)
        elif choice == "5":
            handle_reverse_rollback(fs)
            time.sleep(2)
        elif choice == "6":
            print("\nExiting VCFS...")
            break
        else:
            print("\nInvalid choice. Please enter a number between 1-6.")
            time.sleep(1)
    
    print("Cleaning up...")
    
    # Unmount the filesystem
    try:
        os.system(f"fusermount -u {mount_point}")
        print("Filesystem unmounted.")
    except Exception as e:
        print(f"Error unmounting: {e}")
    
    time.sleep(2)
    
    # Delete backing directory and mount point
    try:
        os.system(f"rm -rf {backing_dir}")
        os.system(f"rm -rf {mount_point}")
        print("Backing directory and mount point deleted.")
    except Exception as e:
        print(f"Error cleaning up directories: {e}")
    
    print("Program terminated.")
    time.sleep(1)
    t.join()
