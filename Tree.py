from typing import Optional

class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right



def maxDepth(root: Optional[TreeNode]) -> int:
    if root is None:
        return 0

    def helper(a, b):

        return root

    depth = 1
    result = helper(root, depth)
    return result

if __name__ == '__main__':
    list_val : list[int] = []
    root = TreeNode(1)
    root.left = TreeNode(2)
    root.right = TreeNode(3)
    root.left.left = TreeNode(4)
    root.left.right = TreeNode(5)
    root.right.right = TreeNode(6)
    root.right.right.right = TreeNode(7)
    root.right.right.right.right = TreeNode(8)
    ans = maxDepth(root)
    print(ans)