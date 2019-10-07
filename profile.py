"""This is a trivial example of a gitrepo-based profile; The profile source code and other software, documentation, etc. are stored in in a publicly accessible GIT repository (say, github.com). When you instantiate this profile, the repository is cloned to all of the nodes in your experiment, to `/local/repository`.
This particular profile is a simple example of using a single raw PC. It can be instantiated on any cluster; the node will boot the default operating system, which is typically a recent version of Ubuntu.
Instructions:
Wait for the profile instance to start, then click on the node in the topology and choose the `shell` menu item.
"""

import geni.portal as portal
import geni.rspec.pg as rspec

# Create a Request object to start building the RSpec.
request = portal.context.makeRequestRSpec()

node = request.RawPC("node")

node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD"

node.cores = 16

node.ram = 1024 * 32

node.disk = 1000

portal.context.printRequestRSpec()
