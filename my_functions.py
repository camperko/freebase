@outputSchema('(subject:chararray,predicate:chararray,p_object:chararray)')
# clear_data - function clears data from url and unnecesary characters to create more readable data
def clear_data(subject, predicate, p_object):
	subject = subject[1:-1].split('/')[-1]
	predicate = predicate[1:-1].split('/')[-1]
	if '"' in p_object:
		quotation = [pos for pos, char in enumerate(p_object) if char == '"']
		p_object = p_object[quotation[0] + 1:quotation[1]]
	else:
		p_object = p_object[1:-1].split('/')[-1]
	if 'type.object.type' in predicate:
		predicate = ''
		p_object = p_object.replace('.', ' ')
	else:
		predicate = predicate.replace('.', ' ')
	return subject, predicate, p_object