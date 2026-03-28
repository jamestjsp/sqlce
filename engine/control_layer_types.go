package engine

// ControlLayerResult holds output of all 8 control-layer queries.
type ControlLayerResult struct {
	ControlMatrix      []ControlMatrixRow      `json:"control_matrix"`
	CVRoleConstraints  []CVRoleConstraintRow   `json:"cv_role_constraints"`
	EconomicFunctions  []EconomicFunctionRow   `json:"economic_functions"`
	VariableTransforms []VariableTransformRow  `json:"variable_transforms"`
	ModelMetadata      []ModelMetadataRow      `json:"model_metadata"`
	ExecutionSequence  []ExecutionSequenceRow  `json:"execution_sequence"`
	UserParameters     []UserParameterRow      `json:"user_parameters"`
	LoopDetails        []LoopDetailRow         `json:"loop_details"`
}

// ControlMatrixRow is Q1: control layer transfer-function matrix.
type ControlMatrixRow struct {
	AppName          string `json:"app_name"`
	BlockType        string `json:"block_type"`
	BlockName        string `json:"block_name"`
	BlockDescription string `json:"block_description"`
	BlockModelHorizon int32 `json:"block_model_horizon"`
	BlockLargestTSS   int32 `json:"block_largest_tss"`
	BlockSmallestTSS  int32 `json:"block_smallest_tss"`

	BLCConfiguration *string `json:"blc_configuration"`
	BLCIntendedMV    *string `json:"blc_intended_mv"`
	BLCLoopType      *string `json:"blc_loop_type"`
	BLCStatus        *string `json:"blc_status"`

	PV       *string `json:"pv"`
	SP       *string `json:"sp"`
	OP       *string `json:"op"`
	LoopMODE *string `json:"loop_mode"`
	LoopINIT *string `json:"loop_init"`

	LoopRanking      *string `json:"loop_ranking"`
	LoopDCSSystem    *string `json:"loop_dcs_system"`
	LoopPIDAlgorithm *string `json:"loop_pid_algorithm"`
	LoopPIDForm      *string `json:"loop_pid_form"`
	LoopPIDEquation  *string `json:"loop_pid_equation"`
	LoopPVTrack      *bool   `json:"loop_pv_track"`

	IsDisturbance    bool   `json:"is_disturbance"`
	SisoElementName  string `json:"siso_element_name"`
	SisoElementType  string `json:"siso_element_type"`

	InputRoleType        string  `json:"input_role_type"`
	InputName            string  `json:"input_name"`
	InputEU              string  `json:"input_eu"`
	InputDescription     string  `json:"input_description"`
	InputNormalMove      float64 `json:"input_normal_move"`
	InputMeasurementType string  `json:"input_measurement_type"`

	OutputRoleType        string  `json:"output_role_type"`
	OutputName            string  `json:"output_name"`
	OutputEU              string  `json:"output_eu"`
	OutputDescription     string  `json:"output_description"`
	OutputNormalMove      float64 `json:"output_normal_move"`
	OutputMeasurementType string  `json:"output_measurement_type"`

	TransferFunction    string  `json:"transfer_function"`
	ElementIsActive     bool    `json:"element_is_active"`
	Delay               float64 `json:"delay"`
	Gain                float64 `json:"gain"`
	Tau1                float64 `json:"tau1"`
	Tau2                float64 `json:"tau2"`
	Beta                float64 `json:"beta"`
	UncertaintyZoneTau  float64 `json:"uncertainty_zone_tau"`
	UncertaintyZoneBeta float64 `json:"uncertainty_zone_beta"`
	BetaXGain           float64 `json:"beta_x_gain"`
	UnitMinY            float64 `json:"unit_min_y"`
	UnitMaxY            float64 `json:"unit_max_y"`
	TSS                 int32   `json:"tss"`
	SamplingTime        float64 `json:"sampling_time"`
	SettlingTimeControl float64 `json:"settling_time_control"`
}

// CVRoleConstraintRow is Q2.
type CVRoleConstraintRow struct {
	VariableName        string  `json:"variable_name"`
	VariableDescription string  `json:"variable_description"`
	RoleType            string  `json:"role_type"`
	IsLoLimitSpec       bool    `json:"is_lo_limit_spec"`
	IsHiLimitSpec       bool    `json:"is_hi_limit_spec"`
	IsSetPointSpec      bool    `json:"is_set_point_spec"`
	MinTimeToLimit      float64 `json:"min_time_to_limit"`
	RampRateImbalanceLo float64 `json:"ramp_rate_imbalance_lo"`
	RampRateImbalanceHi float64 `json:"ramp_rate_imbalance_hi"`
	RampImbalanceMethod string  `json:"ramp_imbalance_method"`
}

// EconomicFunctionRow is Q3.
type EconomicFunctionRow struct {
	ControllerName   string `json:"controller_name"`
	ObjectiveFormula string `json:"objective_formula"`
	IsFormulaValid   bool   `json:"is_formula_valid"`
}

// VariableTransformRow is Q4.
type VariableTransformRow struct {
	VariableName string  `json:"variable_name"`
	Formula      string  `json:"formula"`
	Min          float64 `json:"min"`
	Max          float64 `json:"max"`
	PointNumber  int32   `json:"point_number"`
}

// ModelMetadataRow is Q5.
type ModelMetadataRow struct {
	ModelName                    string `json:"model_name"`
	ModelDescription             string `json:"model_description"`
	ModelHorizonInSeconds        int32  `json:"model_horizon_in_seconds"`
	PlotIntervalInSeconds        int32  `json:"plot_interval_in_seconds"`
	LargestSettlingTimeInSeconds int32  `json:"largest_settling_time_in_seconds"`
	SmallestSettlingTimeInSeconds int32  `json:"smallest_settling_time_in_seconds"`
}

// ExecutionSequenceRow is Q6.
type ExecutionSequenceRow struct {
	ExecutionSequenceIdentifier        string `json:"execution_sequence_identifier"`
	IsDefault                          bool   `json:"is_default"`
	ExecutionIntervalInMilliseconds    int32  `json:"execution_interval_in_milliseconds"`
}

// UserParameterRow is Q7.
type UserParameterRow struct {
	ParameterName        string `json:"parameter_name"`
	ParameterDescription string `json:"parameter_description"`
	EngineeringUnits     string `json:"engineering_units"`
	UserParameterType    string `json:"user_parameter_type"`
	IsOperatorEditable   bool   `json:"is_operator_editable"`
	IsLocal              bool   `json:"is_local"`
}

// LoopDetailRow is Q8.
type LoopDetailRow struct {
	LoopName        string  `json:"loop_name"`
	LoopDescription string  `json:"loop_description"`
	PV              *string `json:"pv"`
	SP              *string `json:"sp"`
	OP              *string `json:"op"`
	MODE            *string `json:"mode"`
	INIT            *string `json:"init"`
	Ranking         *string `json:"ranking"`
	DCSSystem       *string `json:"dcs_system"`
	PIDAlgorithm    *string `json:"pid_algorithm"`
	PIDForm         *string `json:"pid_form"`
	PIDEquation     *string `json:"pid_equation"`
	LoopStatus      *string `json:"loop_status"`
	PVTrack         *bool   `json:"pv_track"`
	SlaveLoopName   *string `json:"slave_loop_name"`
}
