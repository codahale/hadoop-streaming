import sys, unittest
sys.path.insert(0, 'lib')

def fixedGetTestCaseNames(self, testCaseClass):
  """Return a sorted sequence of method names found within testCaseClass
  """
  def isTestMethod(attrname, testCaseClass=testCaseClass, prefix=self.testMethodPrefix):
    attr = getattr(testCaseClass, attrname)
    if attrname.startswith(prefix) and callable(attr):
      return True
    return hasattr(attr, "_unittest_test")
  testFnNames = filter(isTestMethod, dir(testCaseClass))
  for baseclass in testCaseClass.__bases__:
      for testFnName in self.getTestCaseNames(baseclass):
          if testFnName not in testFnNames:  # handle overridden methods
              testFnNames.append(testFnName)
  if self.sortTestMethodsUsing:
      testFnNames.sort(self.sortTestMethodsUsing)
  return testFnNames
unittest.TestLoader.getTestCaseNames = fixedGetTestCaseNames

def test(f):
  f._unittest_test = True
  return f