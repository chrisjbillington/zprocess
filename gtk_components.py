#####################################################################
#                                                                   #
# gtk_components.py                                                 #
#                                                                   #
# Copyright 2013, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################

import threading
import gtk
import pango

import zmq

class OutputBox(object):
    def __init__(self, container):
    
        self.output_view = gtk.TextView()
        container.add(self.output_view)
        self.output_adjustment = self.output_view.get_vadjustment()
        self.output_buffer = self.output_view.get_buffer()
        self.text_mark = self.output_buffer.create_mark(None, self.output_buffer.get_end_iter())
        
        self.output_view.modify_base(gtk.STATE_NORMAL, gtk.gdk.color_parse('black'))
        self.output_view.modify_text(gtk.STATE_NORMAL, gtk.gdk.color_parse('white'))
        self.output_view.modify_font(pango.FontDescription("monospace 10"))
        self.output_view.set_indent(5)
        self.output_view.set_wrap_mode(gtk.WRAP_CHAR)
        self.output_view.set_editable(False)
        self.output_view.show()
                
        context = zmq.Context.instance()
        socket = context.socket(zmq.PULL)
        socket.setsockopt(zmq.LINGER, 0)
        self.port = socket.bind_to_random_port('tcp://127.0.0.1')
        
        # Tread-local storage so we can have one push_sock per
        # thread. push_sock is for sending data to the output queue in
        # a non-blocking way from the same process as this object is
        # instantiated in.  Providing the function OutputBox.output()
        # for this is much easier than expecting every thread to have
        # its own push socket that the user has to manage. Also we can't
        # give callers direct access to the output code, because then
        # it matters whether they hold the gtk lock, and we'll either
        # have deadlocks when they already do, or have to have calling
        # code peppered with lock acquisitions. Screw that.
        self.local = threading.local()
        
        self.mainloop = threading.Thread(target=self.mainloop,args=(socket,))
        self.mainloop.daemon = True
        self.mainloop.start()
    
    def new_socket(self):
        # One socket per thread, so we don't have to acquire a lock
        # to send:
        context = zmq.Context.instance()
        self.local.push_sock = context.socket(zmq.PUSH)
        self.local.push_sock.setsockopt(zmq.LINGER, 0)
        self.local.push_sock.connect('tcp://127.0.0.1:%d'%self.port)
        
    def output(self, text,red=False):
        if not hasattr(self.local, 'push_sock'):
            self.new_socket()
        # Queue the output on the socket:
        self.local.push_sock.send_multipart(['stderr' if red else 'stdout',text.encode()])
        
    def mainloop(self,socket):
        while True:
            stream, text = socket.recv_multipart()
            text = text.decode()
            red = (stream == 'stderr')
            with gtk.gdk.lock:
                # Check if the scrollbar is at the bottom of the textview:
                scrolling = self.output_adjustment.value == self.output_adjustment.upper - self.output_adjustment.page_size
                # We need the initial cursor position so we know what range to make red:
                offset = self.output_buffer.get_end_iter().get_offset()
                # Insert the text at the end:
                self.output_buffer.insert(self.output_buffer.get_end_iter(), text)
                if red:
                    start = self.output_buffer.get_iter_at_offset(offset)
                    end = self.output_buffer.get_end_iter()
                    # Make the text red:
                    self.output_buffer.apply_tag(self.output_buffer.create_tag(foreground='red'),start,end)
                    self.output_buffer.apply_tag(self.output_buffer.create_tag(weight=pango.WEIGHT_BOLD),start,end)

                # Automatically keep the textbox scrolled to the bottom, but
                # only if it was at the bottom to begin with. If the user has
                # scrolled up we won't jump them back to the bottom:
                if scrolling:
                    end_iter = self.output_buffer.get_end_iter()
                    # Move the iter forward to account for the fact that lines might be wrapped:
                    self.output_view.forward_display_line_end(end_iter)
                    end_mark = self.output_buffer.create_mark(None, end_iter)
                    self.output_view.scroll_to_mark(end_mark,0)
