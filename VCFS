import os
import errno
from fuse import FuseOSError
from diff_match_patch import diff_match_patch
from vcfs_base import VCFSBase


class VCFS(VCFSBase):
    def write(self, path, buf, offset, fh):
        full_path = self._full_path(path)
        rel_path = path.lstrip("/")
        safe_name = rel_path.replace("/", "__")

        diff_file = os.path.join(self.meta_dir, f"{safe_name}.diffs")
        count_file = os.path.join(self.meta_dir, f"{safe_name}.count")
        forward_file = os.path.join(self.meta_dir, f"{safe_name}.forward")  # For forward rollback stack

        threshold = 30

        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                old_text = f.read()
        except Exception:
            old_text = ""

        os.lseek(fh, offset, os.SEEK_SET)
        written = os.write(fh, buf)

        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                new_text = f.read()
        except Exception:
            new_text = ""

        # Clear forward rollback stack when new content is written
        if os.path.exists(forward_file):
            os.remove(forward_file)

        count = 0
        if os.path.exists(count_file):
            try:
                with open(count_file, 'r') as cf:
                    count = int(cf.read())
            except Exception:
                pass

        count += 1

        if count >= threshold:
            with open(diff_file, 'w', encoding='utf-8') as df:
                df.write("")
            with open(count_file, 'w') as cf:
                cf.write("0")
        else:
            dmp = diff_match_patch()
            diffs = dmp.diff_main(old_text, new_text)
            dmp.diff_cleanupEfficiency(diffs)
            patch = dmp.patch_toText(dmp.patch_make(new_text, old_text))

            if patch:
                with open(diff_file, 'a', encoding='utf-8') as df:
                    df.write(f"--- VERSION {safe_name} ---\n")
                    df.write(patch + "\n")

            with open(count_file, 'w') as cf:
                cf.write(str(count))

        return written

    def rollback(self, path):
        full_path = self._full_path(path)
        rel_path = path.lstrip("/")
        safe_name = rel_path.replace("/", "__")

        diff_file = os.path.join(self.meta_dir, f"{safe_name}.diffs")
        count_file = os.path.join(self.meta_dir, f"{safe_name}.count")
        forward_file = os.path.join(self.meta_dir, f"{safe_name}.forward")

        if not os.path.exists(diff_file):
            raise FuseOSError(errno.ENOENT)

        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                current_text = f.read()
        except Exception:
            current_text = ""

        with open(diff_file, 'r', encoding='utf-8') as df:
            lines = df.readlines()

        version_indexes = [i for i, line in enumerate(lines) if line.startswith("--- VERSION")]
        if not version_indexes:
            print("No previous versions found to rollback.")
            return

        last_index = version_indexes[-1]
        patch_lines = lines[last_index + 1:]

        dmp = diff_match_patch()
        patch = dmp.patch_fromText("".join(patch_lines))
        reverted_text, results = dmp.patch_apply(patch, current_text)

        if not all(results):
            print("Patch failed to apply completely.")
            raise FuseOSError(errno.EIO)

        # Store current version for forward rollback (append to stack)
        with open(forward_file, 'a', encoding='utf-8') as ff:
            ff.write(f"--- FORWARD VERSION {safe_name} ---\n")
            ff.write(current_text + "\n--- END FORWARD VERSION ---\n")

        # Apply the rollback
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(reverted_text)

        # Remove the used patch from diff file
        with open(diff_file, 'w', encoding='utf-8') as df:
            df.writelines(lines[:last_index])

        if os.path.exists(count_file):
            try:
                with open(count_file, 'r+') as cf:
                    count = int(cf.read())
                    cf.seek(0)
                    cf.write(str(max(0, count - 1)))
                    cf.truncate()
            except Exception:
                pass

        print(f"Rollback applied on {path}. Newer version saved for possible reverse rollback.")

    def reverse_rollback(self, path):
        full_path = self._full_path(path)
        rel_path = path.lstrip("/")
        safe_name = rel_path.replace("/", "__")
        forward_file = os.path.join(self.meta_dir, f"{safe_name}.forward")
        diff_file = os.path.join(self.meta_dir, f"{safe_name}.diffs")
        count_file = os.path.join(self.meta_dir, f"{safe_name}.count")

        if not os.path.exists(forward_file):
            print("No newer version available for reverse rollback.")
            return

        # Read current content before reverse rollback
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                current_text = f.read()
        except Exception:
            current_text = ""

        # Read all forward versions
        with open(forward_file, 'r', encoding='utf-8') as ff:
            content = ff.read()

        # Parse forward versions (they're stored in reverse chronological order)
        forward_versions = []
        sections = content.split(f"--- FORWARD VERSION {safe_name} ---\n")
        
        for section in sections[1:]:  # Skip first empty section
            if "--- END FORWARD VERSION ---" in section:
                version_content = section.split("--- END FORWARD VERSION ---")[0].rstrip('\n')
                forward_versions.append(version_content)

        if not forward_versions:
            print("No newer version available for reverse rollback.")
            return

        # Get the most recent forward version (last one added)
        restored_text = forward_versions[-1]

        # Create patch from current to restored version and add it back to diffs
        dmp = diff_match_patch()
        diffs = dmp.diff_main(current_text, restored_text)
        dmp.diff_cleanupEfficiency(diffs)
        patch = dmp.patch_toText(dmp.patch_make(restored_text, current_text))

        if patch:
            # Add the patch back to the diffs file
            with open(diff_file, 'a', encoding='utf-8') as df:
                df.write(f"--- VERSION {safe_name} ---\n")
                df.write(patch + "\n")

            # Increment count
            count = 0
            if os.path.exists(count_file):
                try:
                    with open(count_file, 'r') as cf:
                        count = int(cf.read())
                except Exception:
                    pass
            
            with open(count_file, 'w') as cf:
                cf.write(str(count + 1))

        # Write restored content to the file
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(restored_text)

        # Remove the used forward version from the stack
        if len(forward_versions) == 1:
            # If this was the last forward version, remove the file
            os.remove(forward_file)
        else:
            # Otherwise, rewrite the file without the last version
            with open(forward_file, 'w', encoding='utf-8') as ff:
                for version in forward_versions[:-1]:
                    ff.write(f"--- FORWARD VERSION {safe_name} ---\n")
                    ff.write(version + "\n--- END FORWARD VERSION ---\n")

        print(f"Reverse rollback applied on {path}. Rollback patch restored to versions file.")
