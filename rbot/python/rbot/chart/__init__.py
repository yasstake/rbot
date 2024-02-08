
from collections import OrderedDict
import numpy as np
import pandas as pd

from bokeh.layouts import column 
from bokeh.models import ColumnDataSource, RangeTool, HoverTool, CrosshairTool, Span
from bokeh.plotting import figure
from bokeh.io import output_notebook, show
import datetime


class Chart:
    def __init__(self, width, height, ohlcv):
        output_notebook()        
        
        self.figure = OrderedDict()
        self.width = width
        self.x_range = None

        ######### create main price figure ############
        TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

        # setup main figure
        dates = np.array(ohlcv.index, dtype=datetime.datetime)        
        price = figure(x_axis_type="datetime", tools=TOOLS, width=self.width, height=height,
           title="Price", background_fill_color="#efefef", x_range=(dates[0], dates[-1]))
        
        self.x_range = price.x_range
        self.figure['price'] = price
        
        self.draw_ohlc(price, ohlcv)

        span_height = Span(dimension="height", line_dash="dashed", line_width=1)
        self.cross_hair = CrosshairTool(overlay=span_height)        
        price.add_tools(self.cross_hair)

        ########  create volume figure ################
        if 'volume' in ohlcv.columns:
            volume = self.new_figure('Volume', 100, 'Volume')
            self.draw_volume('Volume', ohlcv)
            volume.add_tools(self.cross_hair)

        ######### setup select figure #################
        """ Disable range tools
        select = figure(title="Price slide bar",
                height= int(height/4), width=self.width, y_range=price.y_range,
                x_axis_type="datetime", y_axis_type=None,
                tools="", toolbar_location=None, background_fill_color="#efefef")            

        self.select = select

        self.line(select, ohlcv, x_key='timestamp', y_key='close', legend_label='price', color='#1010ff')
        
        range_tool = RangeTool(x_range=self.x_range)
        range_tool.overlay.fill_color = "navy"
        range_tool.overlay.fill_alpha = 0.2

        #select.ygrid.grid_line_color = None
        #select.add_tools(range_tool)
        #select.toolbar.active_multi = range_tool
        #select.add_tools(self.cross_hair)
        """
    
    def new_figure(self, name, height, title):
        p = figure(x_axis_type="datetime", width=self.width, height=height, tools="", toolbar_location=None,
            title=title, background_fill_color="#efefef", x_range=self.x_range)
        self.figure[name] = p
        p.add_tools(self.cross_hair)

        return p 
        
    def get_figure(self, figure):
        if isinstance(figure, str):
            if figure in self.figure:
                return self.figure[figure]
        
            return self.new_figure(figure, 100, figure)
        else:
            return figure

    def show(self):
        figure = []
        for key in self.figure:
            figure.append(self.figure[key])

        # remove select figure
        #figure.append(self.select)
            
        show(column(figure))

    def draw_result(self, df):
        buy_df = df[(df['order_side'] == "Buy") & (df['status'] != 'Expire')]
        buy_df_e = df[(df['order_side'] == "Buy") & (df['status'] == 'Expire')]
        sell_df = df[(df['order_side'] == "Sell") & (df['status'] != 'Expire')]
        sell_df_e = df[(df['order_side'] == "Sell") & (df['status'] == 'Expire')]

        p = self.new_figure('profit', 150, 'profit')
        self.step(p, df, 'update_time', 'sum_profit', legend_label='profit', color="#ff8080")

        p = self.new_figure('position', 100, 'position')
        self.step(p, df, 'update_time', 'position', legend_label='position', color="#ff8080")

        p = self.get_figure('price')
        self.draw_order_maker(p, buy_df, 'triangle', fill_color="#00ff00", line_color="#00ff00", legend_name='Buy')
        self.draw_order_maker(p, sell_df, 'inverted_triangle', fill_color="#ff0000", line_color="#ff0000", legend_name='Sell')
        self.draw_order_maker(p, buy_df_e, 'triangle', fill_color="#00000000", line_color="#00ff00", legend_name='Buy Expire')
        self.draw_order_maker(p, sell_df_e, 'inverted_triangle', fill_color="#00000000", line_color="#ff0000", legend_name='Sell Expire')


    def draw_ohlc(self, p, ohlc):
        ds = ColumnDataSource(ohlc)

        df_inc = ColumnDataSource(ohlc[(ohlc['open'] <= ohlc['close'])])
        df_dec = ColumnDataSource(ohlc[(ohlc['close'] < ohlc['open'])])

        delta = (ohlc[1:2].index - ohlc[0:1].index)[0]
        w = delta.total_seconds() * 1_000 * 0.8

        p.segment('timestamp', 'high', 'timestamp', 'low', source=ds, color="#080808")
        vbar_dec = p.vbar('timestamp', w, 'close', 'open', source=df_dec, fill_color='#ff66ff', line_color='#ff0000', line_width=0)        
        vbar_inc = p.vbar('timestamp', w, 'open', 'close', source=df_inc, fill_color='#66ccff', line_color="#10ff80", line_width=0)

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
            show_arrow=False,
        )      

        p.add_tools(hover_inc)
        p.add_tools(hover_dec)


    def draw_order_maker(self, p, df, marker, fill_color, line_color, legend_name, **kwargs):
        df = ColumnDataSource(df)
        scatter = p.scatter(x='update_time', y='order_price', source=df, marker=marker, size=12, fill_color=fill_color, line_color=line_color, line_width=1, legend_label=legend_name, **kwargs)

        hover = HoverTool(
            renderers=[scatter],
            tooltips = [
                (legend_name,""),
                ("timestamp", "@update_time{%F %R.%S}"),
                ("Price", "@order_price{0.0}")
            ],
            formatters={
                "@update_time": "datetime"
            }
        )
        p.add_tools(hover)    

    def draw_volume(self, figure, ohlcv):
        p = self.get_figure(figure)        
        self.line(p, ohlcv, x_key='timestamp', y_key='volume', color='#00ffff', legend_label='volume')

    
    def line2(self, figure, df, x_key, y_key, legend_label, color, **kwargs):
        p = self.get_figure(figure)
        df = self.make_df_from_series(df)        

        p.line(x=x_key, y=y_key, source=df, line_color=color, legend_label=legend_label, **kwargs)

    def line(self, figure, df, x_key= None, y_key=None, **kwargs):
        p = self.get_figure(figure)
        df = self.make_df_from_series(df)        
        
        if not x_key:
            x_key = 'timestamp'
        
        if not y_key:
            y_key = 'value'

        p.line(x=x_key, y=y_key, source=df, **kwargs)

    
    def step(self, figure, df, x_key=None, y_key=None, **kwargs):
        p = self.get_figure(figure)
        df = self.make_df_from_series(df)

        if not x_key:
            x_key = 'timestamp'
        
        if not y_key:
            y_key = 'value'

        p.step(x=x_key, y=y_key, source=df, mode='after', **kwargs)

    def make_df_from_series(self, df):
        return ColumnDataSource(self.make_df(df))
    
    def make_df(self, df):
        if isinstance(df, pd.DataFrame):
            df = df
        elif isinstance(df, pd.Series):
            df = df.rename('value')
            df = pd.DataFrame(df)
            df.index.name = 'timestamp'

            df.index = pd.to_datetime(df.index, utc=True, unit='us')
            df = df.dropna()
    
        df = df[~df.index.duplicated(keep='last')]
        
        return df

