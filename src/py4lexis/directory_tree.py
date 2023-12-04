from __future__ import annotations
from typing import Generator
from py4lexis.custom_types.directory_tree import TreeDirectoryObject, TreeFileObject


class DirectoryTree(object):
    """
        Class which provides recursive print of directory tree of existing dataset.

        To use it, type

            tree_content = DirectoryObject(content)
            items = DirectoryTree.make_tree(tree_content)
            for item in items:
                item.print()
        
        where content is the JSON content of the HTTP request for list of files, i.e. content inside of function py4lexis.ddi.get_list_of_files_in_dataset().        

        Inspired by Abstrus at https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python

        Attributes
        ----------
        print_name: str
            Name of a file or a directory which will be printed.
        parent: DirectoryTree | None
            Holds the parent of the class.
        is_last: bool
            Wheter the object is last in the directory tree or not.
        depth: int, in_class
            The depth in which the object is held within the directory tree.

        Methods
        -------
        make_tree(cls, tree_content: DirectoryObject | FileObject, parent: DirectoryTree | None=None, is_last: bool=False) -> Generator[DirectoryTree, None, None]
            Creates the directory tree from the content of HTTP response.

        to_string() -> str
            Converts item of the directory tree to a coresponding line.

        to_dataframe_row(self) -> list[str | int] | None:
            Converts directory tree item into DataFrame row.
    """
    item_prefix_middle: str = "├──"
    item_prefix_last: str = "└──"
    parent_prefix_middle: str = "    "
    parent_prefix_last: str = "│   "

    def __init__(self, tree_item: TreeDirectoryObject | TreeFileObject, parent: DirectoryTree | None, is_last: bool) -> None:
        self.tree_item: TreeDirectoryObject | TreeFileObject = tree_item
        self.parent: DirectoryTree | None = parent
        self.is_last: bool = is_last
        if self.parent:
            self.depth: int = self.parent.depth + 1
        else:
            self.depth: int = 0

    @classmethod
    def make_tree(cls, tree_content: TreeDirectoryObject | TreeFileObject, parent: object | None=None, is_last: bool=False) -> Generator[DirectoryTree, None, None]:
        """
            Creates the directory tree from the content of HTTP response.

            Parameters
            ----------
            tree_content: DirectoryObject | FileObject
                Object (dictionary) within the content of the HTTP response.
            parent: DirectoryTree | None, optional
                Parent of the content in the directory tree.
            is_last: bool, optional
                Wheter the object is last in the directory tree or not.

            Returns
            -------
            None
        """
        
        displayable_root: DirectoryTree = cls(tree_content, parent, is_last)
        yield displayable_root

        children: list[TreeDirectoryObject | TreeFileObject] = []
        for item in tree_content.contents:
            if item["type"] == "directory":
                children.append(TreeDirectoryObject(item))
            else:
                children.append(TreeFileObject(item))

        count: int = 1
        for item in children:
            is_last: bool = count == len(children)
            if item.is_dir():
                yield from cls.make_tree(item,
                                         parent=displayable_root,
                                         is_last=is_last)
            else:
                yield cls(item, displayable_root, is_last)
            count += 1


    def to_string(self) -> str:
        """
            Converts item of the directory tree to a coresponding line.

            Returns
            -------
            str
                A line in the directory tree as string.
        """

        if self.parent is None:
            return self.tree_item.name

        _filename_prefix: str = (self.item_prefix_last if self.is_last else self.item_prefix_middle)

        parts: list[str] = ["{!s} {!s}".format(_filename_prefix, self.tree_item.name)]

        parent: DirectoryTree | None = self.parent
        while parent and parent.parent is not None:
            parts.append(self.parent_prefix_middle if parent.is_last else self.parent_prefix_last)
            parent = parent.parent

        return "".join(reversed(parts))
    
    def to_dataframe_row(self) -> list[str | int] | None:
        """
            Converts directory tree item into DataFrame row.

            Returns
            -------
            list[str | int] | None
                If object is not a directory then it returns a row for DataFrame table.
        """

        if self.tree_item.is_dir():
            return None
        else:
            parts: list[str] = ["{!s}".format(self.tree_item.name)]

            parent: DirectoryTree | None = self.parent
            while parent and parent.parent is not None:
                parts.append(self.parent.tree_item.name)
                parent = parent.parent

            parts.append("./")
            path: str = "".join(reversed(parts))
            
            return [self.tree_item.name, path, self.tree_item.size, self.tree_item.create_time, self.tree_item.checksum]