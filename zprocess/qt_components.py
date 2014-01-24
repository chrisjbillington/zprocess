#####################################################################
#                                                                   #
# qt_components.py                                                  #
#                                                                   #
# Copyright 2013, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################

# This file is included for backward compatability, but the OutputBox
# class has been moved to the qtutils project for licensing reasons.
# A longer term aim is to remove GUI elements from zprocess, but they
# are kept here for the moment for compatability.

from qtutils.outputbox import OutputBox
