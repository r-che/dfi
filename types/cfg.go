package types

type CommonFlags struct {
	UseTags		bool	// Affects query to DB
	UseDescr	bool	// Affects query to DB
}

type QueryFlags struct {
	OrExpr		bool
	NegExpr		bool
	OnlyName	bool
	OnlyTags	bool
	OnlyDescr	bool
	DeepSearch	bool
}
