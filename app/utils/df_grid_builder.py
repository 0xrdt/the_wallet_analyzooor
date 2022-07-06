from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode
from st_aggrid import GridUpdateMode, DataReturnMode

def df_grid_builder(df):
	gb = GridOptionsBuilder.from_dataframe(df)
	# enables pivoting on all columns, however i'd need to change ag grid to allow export of pivoted/grouped data, however it select/filters groups
	gb.configure_default_column(enablePivot=True, enableValue=True, enableRowGroup=True)
	gb.configure_selection(selection_mode="multiple", use_checkbox=True)
	gb.configure_side_bar()  # side_bar is clearly a typo :) should by sidebar
	gridOptions = gb.build()
	response = AgGrid(
		df,
		gridOptions=gridOptions,
		enable_enterprise_modules=True,
		update_mode=GridUpdateMode.MODEL_CHANGED,
		data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
		fit_columns_on_grid_load=False,
	)