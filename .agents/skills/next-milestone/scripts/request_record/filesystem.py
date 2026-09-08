"""Pinned directory ownership and secure descriptor-relative file operations."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

from . import contract


class StatKey(NamedTuple):
    device: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int
    mode: int


@dataclass
class PinnedDirectory:
    root_path: Path
    project_path: Path
    leaf_path: Path
    root_fd: int
    project_fd: int
    leaf_fd: int

    def matches_visible_chain(self) -> bool:
        for path, descriptor in (
            (self.root_path, self.root_fd),
            (self.project_path, self.project_fd),
            (self.leaf_path, self.leaf_fd),
        ):
            try:
                opened = os.fstat(descriptor)
                visible = path.lstat()
            except (FileNotFoundError, OSError):
                return False
            if (
                not stat.S_ISDIR(opened.st_mode)
                or stat.S_ISLNK(visible.st_mode)
                or (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino)
            ):
                return False
        return True

    def close(self) -> None:
        os.close(self.leaf_fd)
        os.close(self.project_fd)
        os.close(self.root_fd)


def canonical_projects_root(value: str) -> Path:
    supplied = Path(value)
    if not supplied.is_absolute():
        raise contract.ContractError(
            "unsafe_path", value, "projects root must be absolute"
        )
    try:
        info = supplied.lstat()
    except FileNotFoundError as exc:
        raise contract.ContractError(
            "unsafe_path", value, "projects root does not exist"
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise contract.ContractError(
            "unsafe_path", value, "projects root must be a real directory"
        )
    return supplied.resolve(strict=True)


def require_real_directory(path: Path, allow_missing: bool = False) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        if allow_missing:
            return
        raise contract.ContractError(
            "unsafe_path", str(path), "required directory does not exist"
        )
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise contract.ContractError(
            "unsafe_path", str(path), "path component must be a real directory"
        )


def require_real_file(path: Path) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise contract.ContractError(
            "missing_record", str(path), "required file does not exist"
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise contract.ContractError(
            "unsafe_path", str(path), "path must be a regular non-symlink file"
        )


def open_directory_fd(path: Path) -> int:
    """Open and pin a real directory, rejecting a path-to-inode race."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise contract.ContractError(
            "unsafe_path",
            str(path),
            "directory could not be opened without symlink traversal",
        ) from exc
    try:
        opened = os.fstat(descriptor)
        current = path.lstat()
        if (
            not stat.S_ISDIR(opened.st_mode)
            or stat.S_ISLNK(current.st_mode)
            or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
        ):
            raise contract.ContractError(
                "unsafe_path", str(path), "directory identity changed during validation"
            )
    except (contract.ContractError, OSError):
        os.close(descriptor)
        raise
    return descriptor


def open_child_directory_fd(
    parent_fd: int, name: str, display_path: Path, create: bool
) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        return os.open(name, flags, dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            raise contract.ContractError(
                "unsafe_path", str(display_path), "required directory does not exist"
            )
        try:
            os.mkdir(name, 0o700, dir_fd=parent_fd)
        except FileExistsError:
            pass
        try:
            return os.open(name, flags, dir_fd=parent_fd)
        except OSError as exc:
            raise contract.ContractError(
                "unsafe_path",
                str(display_path),
                "created directory could not be pinned without symlink traversal",
            ) from exc
    except OSError as exc:
        raise contract.ContractError(
            "unsafe_path",
            str(display_path),
            "directory component could not be opened without symlink traversal",
        ) from exc


def pin_named_directory(
    root: Path, project_name: str, leaf_name: str, create: bool = False
) -> PinnedDirectory:
    if (
        Path(project_name).name != project_name
        or "/" in project_name
        or "\\" in project_name
        or Path(leaf_name).name != leaf_name
    ):
        raise contract.ContractError(
            "unsafe_path", project_name, "directory names must be direct children"
        )
    root_fd = open_directory_fd(root)
    project_path = root / project_name
    leaf_path = project_path / leaf_name
    try:
        project_fd = open_child_directory_fd(
            root_fd, project_name, project_path, create=create
        )
    except (contract.ContractError, OSError):
        os.close(root_fd)
        raise
    try:
        leaf_fd = open_child_directory_fd(
            project_fd, leaf_name, leaf_path, create=create
        )
    except (contract.ContractError, OSError):
        os.close(project_fd)
        os.close(root_fd)
        raise
    pinned = PinnedDirectory(
        root_path=root,
        project_path=project_path,
        leaf_path=leaf_path,
        root_fd=root_fd,
        project_fd=project_fd,
        leaf_fd=leaf_fd,
    )
    if not pinned.matches_visible_chain():
        pinned.close()
        raise contract.ContractError(
            "unsafe_path",
            str(leaf_path),
            "project directory chain changed while it was being pinned",
        )
    return pinned


def pin_project_directory(
    root: Path, target_repo: str, leaf_name: str, create: bool = False
) -> PinnedDirectory:
    contract.validate_kebab(target_repo, target_repo, "target_repo")
    return pin_named_directory(root, target_repo, leaf_name, create)


def entry_identity_at(directory_fd: int, filename: str) -> tuple[int, int] | None:
    try:
        info = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    return info.st_dev, info.st_ino


def unlink_owned_at(
    directory_fd: int, filename: str, owned_identity: tuple[int, int]
) -> bool:
    """Remove a helper-created entry only while its visible inode is still ours."""
    if entry_identity_at(directory_fd, filename) != owned_identity:
        return False
    try:
        os.unlink(filename, dir_fd=directory_fd)
    except FileNotFoundError:
        return False
    return True


def request_path(
    root: Path, target_repo: str, filename: str, create_dirs: bool = False
) -> Path:
    contract.validate_kebab(target_repo, target_repo, "target_repo")
    contract.validate_filename(filename, filename)
    project = root / target_repo
    requests = project / "requests"
    if project.parent != root or requests.parent != project:
        raise contract.ContractError(
            "unsafe_path", str(project), "project path is not a direct child"
        )
    require_real_directory(project, allow_missing=create_dirs)
    require_real_directory(requests, allow_missing=create_dirs)
    destination = requests / filename
    if destination.parent != requests:
        raise contract.ContractError(
            "unsafe_path", str(destination), "request destination is not a direct child"
        )
    if destination.exists() or destination.is_symlink():
        require_real_file(destination)
    return destination


def response_path(root: Path, target_repo: str, filename: str) -> Path:
    contract.validate_kebab(target_repo, target_repo, "target_repo")
    contract.validate_filename(filename, filename)
    project = root / target_repo
    responses = project / "responses"
    require_real_directory(project)
    require_real_directory(responses)
    destination = responses / filename
    require_real_file(destination)
    return destination


def snapshot_at(
    directory_fd: int, filename: str, display_path: Path
) -> tuple[bytes, StatKey]:
    flags = os.O_RDONLY | os.O_NONBLOCK
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(filename, flags, dir_fd=directory_fd)
    except OSError as exc:
        raise contract.ContractError(
            "unsafe_path",
            str(display_path),
            "request could not be opened relative to its validated directory",
        ) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise contract.ContractError(
                "unsafe_path", str(display_path), "opened request is not a regular file"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
        before_key = StatKey(
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
            stat.S_IMODE(before.st_mode),
        )
        after_key = StatKey(
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
            stat.S_IMODE(after.st_mode),
        )
        if before_key != after_key:
            raise contract.ContractError(
                "digest_mismatch", str(display_path), "request changed while being read"
            )
        return b"".join(chunks), after_key
    finally:
        os.close(descriptor)
