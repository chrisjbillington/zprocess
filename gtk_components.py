import threading
import gtk
import pango

class OutputBox(object):
    def __init__(self,container, queue):
    
        self.output_view = gtk.TextView()
        container.add(self.output_view)
        self.output_adjustment = self.output_view.get_vadjustment()
        self.output_buffer = self.output_view.get_buffer()
        self.text_mark = self.output_buffer.create_mark(None, self.output_buffer.get_end_iter())
        
        self.output_view.modify_base(gtk.STATE_NORMAL, gtk.gdk.color_parse('black'))
        self.output_view.modify_text(gtk.STATE_NORMAL, gtk.gdk.color_parse('white'))
        self.output_view.modify_font(pango.FontDescription("monospace 10"))
        self.output_view.set_indent(5)
        self.output_view.set_wrap_mode(gtk.WRAP_WORD_CHAR)
        self.output_view.set_editable(False)
        self.output_view.show()
                
        self.queue = queue
        self.mainloop = threading.Thread(target=self.mainloop)
        self.mainloop.daemon = True
        self.mainloop.start()
        
    def mainloop(self):
        while True:
            stream, text = self.queue.get()
            if stream == 'stderr':
                red = True
            else:
                red = False
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
