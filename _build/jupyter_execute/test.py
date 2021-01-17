# The Great Red Spot

Jupiter’s iconic Great Red Spot (GRS) is actually an enormous storm that is bigger than Earth that has raged for hundreds of years! {numref}`great-red-spot` below shows an image of Jupiter captured by the Hubble Space Telescope on June 27, 2019.

```{figure} https://solarsystem.nasa.gov/system/resources/detail_files/626_PIA21775.jpg
---
height: 300px
name: great-red-spot
---
Jupiter's Great Red Spot! Source: [NASA](https://solarsystem.nasa.gov/resources/626/jupiters-great-red-spot-in-true-color/?category=planets_jupiter).
```

Jupiter's GRS has been observed to be shrinking for about the last century and a half! [Here](https://github.com/UBC-DSCI/jupyterdays/tree/master/jupyterdays/sessions/beuzen/data) is some data of the length of the GRS spanning the last ~150 years which we can use to investigate this phenomenon.

import pandas as pd
pd.options.plotting.backend = "plotly"

url = "https://raw.githubusercontent.com/UBC-DSCI/jupyterdays/master/jupyterdays/sessions/beuzen/data/GRS_data.csv"
df = pd.read_csv(url)
df['Year'] = df['Year'].astype(int) 
df.head()

import plotly.io as pio
pio.renderers.default = "notebook"
fig = df.plot.scatter(x="Year", y="GRS Length", color="Recorder",
                      range_x=[1870, 2030], range_y=[10, 40],
                      width=650, height=400)
fig.update_layout(title={'text': "Great Red Spot Size", 'x':0.5, 'y':0.92})
fig.update_traces(marker=dict(size=7))

fig = df.plot.scatter(x="Year", y="GRS Length",
                      animation_frame="Year",
                      range_x=[1870, 2030], range_y=[10, 40],
                      width=600, height=520)
fig.update_layout(title={'text': "Great Red Spot Size Animation", 'x':0.5, 'y':0.94})
fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 200
fig.update_traces(marker=dict(size=10))