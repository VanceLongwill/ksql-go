package client

// CommandResult contains information about a CREATE, DROP or TERMINATE command
type CommandResult struct {
	commonResult

	// CommandID is the identified for the requested operation. You can use this ID to poll the result of the operation using the status endpoint.
	CommandID string `json:"commandId,omitempty"`
	// CommandStatus is the status of the requested operation.
	CommandStatus CommandStatus `json:"commandStatus,omitempty"`
}

func (c *CommandResult) is(target ExecResult) bool {
	if target.CommandResult != nil {
		*c = *target.CommandResult
		c.commonResult = target.commonResult
		return true
	}
	return false
}
