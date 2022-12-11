


from collections import OrderedDict
import numpy as np

from bokeh.layouts import column 
from bokeh.models import ColumnDataSource, RangeTool, HoverTool
from bokeh.plotting import figure
from bokeh.io import output_notebook, show

class Chart:
    def __init__(self, width, height, ohlc):
        output_notebook()        
        
        self.figure = OrderedDict()
        self.width = width
        self.x_range = None
        self.select = None

        TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

        # setup main figure
        dates = np.array(ohlc.index, dtype=np.datetime64)        
        main = figure(x_axis_type="datetime", tools=TOOLS, width=self.width, height=height,
           title="BTC chart", background_fill_color="#efefef", x_range=(dates[0], dates[-1]))
        
        self.x_range = main.x_range
        self.figure['main'] = main
        
        self.draw_ohlc(main, ohlc)

        # setup select figure
        select = figure(title="Drag the middle and edges of the selection box to change the range above",
                height= int(height/4), width=self.width, y_range=main.y_range,
                x_axis_type="datetime", y_axis_type=None,
                tools="", toolbar_location=None, background_fill_color="#efefef")            

        self.select = select

        self.draw_price_line(select, ohlc)
        
        range_tool = RangeTool(x_range=self.x_range)
        range_tool.overlay.fill_color = "navy"
        range_tool.overlay.fill_alpha = 0.2

        select.ygrid.grid_line_color = None
        select.add_tools(range_tool)
        select.toolbar.active_multi = range_tool
        

    
    def new_figure(self, name, height, title):
        p = figure(x_axis_type="datetime", width=self.width, height=height, tools="", toolbar_location=None,
            title=title, background_fill_color="#efefef", x_range=self.x_range)
        self.figure[name] = p 
        
    def get_figure(self, name):
        return self.figure[name]

    def show(self):
        figure = []
        for key in self.figure:
            print(key)
            figure.append(self.figure[key])

        figure.append(self.select)
            
        show(column(figure))

    def draw_ohlc(self, p, ohlc):
        ds = ColumnDataSource(ohlc)

        df_inc = ColumnDataSource(ohlc[(ohlc['open'] <= ohlc['close'])])
        df_dec = ColumnDataSource(ohlc[(ohlc['close'] < ohlc['open'])])

        delta = (ohlc[1:2].index - ohlc[0:1].index)[0]
        w = delta.total_seconds() * 1_000 * 0.8

        p.segment('timestamp', 'high', 'timestamp', 'low', source=ds, color="#eb3c40")
        vbar_inc = p.vbar('timestamp', w, 'open', 'close', source=df_inc, fill_color="white", line_color="#49a3a3", line_width=2)
        vbar_dec = p.vbar('timestamp', w, 'close', 'open', source=df_dec)

        hover_inc = HoverTool(
            renderers=[vbar_inc],
            tooltips = [
                ("timestamp", "@timestamp{%F %R.%S}"),
                ("open", "@open{0.0}"),
                ("high", "@high{0.0}"),
                ("low", "@low{0.0}"),
                ("close", "@close{0.0}")
            ],
            formatters= {
                "@timestamp": "datetime",
            },
            mode="vline",
            show_arrow=False,
        )       

        hover_dec = HoverTool(
            renderers=[vbar_dec],
            tooltips = [
                ("timestamp", "@timestamp{%F %R.%S}"),
                ("open", "@open{0.0}"),
                ("high", "@high{0.0}"),
                ("low", "@low{0.0}"),
                ("close", "@close{0.0}")
            ],
            formatters= {
                "@timestamp": "datetime"
            },
            mode="vline",
            show_arrow=False,
        )      

        p.add_tools(hover_inc)
        p.add_tools(hover_dec)

    def draw_price_line(self, p, ohlc):
        p.line(x=ohlc.index, y=ohlc['close'])
        
    def draw_volume_line(self, name, ohlcv):
        p = self.figure[name]
        p.line(x=ohlcv.index, y=ohlcv['volume'])    
        
        
