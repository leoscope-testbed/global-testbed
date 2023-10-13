from common.sly import Lexer, Parser

FIELDS = ['uplink_throughput_bps', 'downlink_throughput_bps', 'exp_tx_rate_bytes', 
			'exp_rx_rate_bytes', 'pop_ping_latency_ms', 'direction_azimuth',
			'direction_elevation', 'currently_obstructed', 'fraction_obstructed',
			'sat_dist_max', 'sat_dist_min', 'sat_dist_mean', 'weather_id', 
			'weather_main', 'weather_desc', 'weather_temp', 'weather_pressure', 
			'weather_humidity', 'weather_temp_min', 'weather_temp_max', 
			'weather_temp_mean', 'weather_clouds', 'weather_rain1h', 
			'weather_rain3h', 'weather_snow1h', 'weather_snow3h', 
			'weather_visibility', 'weather_speed', 'weather_deg', 'weather_gust']

class TriggerLexer(Lexer):
	# tokens = { NAME, NUMBER, STRING }
	tokens = { NAME, NUMBER, EQUAL, INEQUAL, BIGGERE, SMALLERE}
	ignore = '\t '
	literals = { '=', '+', '-', '/',
				'*', '(', ')', ',', ';', '&', '|', '!',
				'>', '<'}

	# Define tokens as regular expressions
	# (stored as raw strings)
	NAME = r'[a-zA-Z_][a-zA-Z0-9_]*'
	# STRING = r'\".*?\"'
	EQUAL = r'(===|==)'
	INEQUAL = r'(!==|!=)'
	BIGGERE = r'(>=)'
	SMALLERE = r'(<=)'

	# Number token
	@_(r'\d+')
	def NUMBER(self, t):
		
		# convert it into a python integer
		t.value = int(t.value)
		return t

	# Comment token
	@_(r'//.*')
	def COMMENT(self, t):
		pass

	# Newline token(used only for showing
	# errors in new line)
	@_(r'\n+')
	def newline(self, t):
		self.lineno = t.value.count('\n')


class TriggerParser(Parser):
	#tokens are passed from lexer to parser
	tokens = TriggerLexer.tokens
	# debugfile = 'parser.out'

	precedence = (
		('left', '|'),
		('left', '&'),
		('left', '!'),
		('left', '<', 'SMALLERE', '>', 'BIGGERE', 'EQUAL', 'INEQUAL'),
		('left', '+', '-'),
		('left', '*', '/'),
		('right', '('),
		('right', ')'),
		('right', 'UMINUS'),
	)
  
	def __init__(self):
		self.env = { }
  
	@_('')
	def bstatement(self, p):
		pass
  
	# @_('var_assign')
	# def statement(self, p):
	#     return p.var_assign
  
	# @_('NAME "=" expr')
	# def var_assign(self, p):
	#     return ('var_assign', p.NAME, p.expr)
  
	# @_('NAME "=" STRING')
	# def var_assign(self, p):
	#     return ('var_assign', p.NAME, p.STRING)
  
	@_('bexpr')
	def bstatement(self, p):
		return (p.bexpr)

	@_('bexpr "|" bexpr1')
	def bexpr(self, p):
		return ('or', p.bexpr, p.bexpr1)
	
	@_('bexpr1')
	def bexpr(self, p):
		return (p.bexpr1)
	
	@_('bexpr1 "&" bexpr2')
	def bexpr1(self, p):
		return ('and', p.bexpr1, p.bexpr2)

	@_('bexpr2')
	def bexpr1(self, p):
		return (p.bexpr2)

	@_('"!" bexpr2')
	def bexpr2(self, p):
		return ('not', p.bexpr2)

	@_('bexpr3')
	def bexpr2(self, p):
		return (p.bexpr3)

	@_('"(" bexpr ")"')
	def bexpr3(self, p):
		return (p.bexpr)

	@_('lstatement')
	def bexpr3(self, p):
		return (p.lstatement) 

	@_('lexpr')
	def lstatement(self, p):
		return (p.lexpr)

	# --- logical statement --- 
	@_('"(" lexpr ")"')
	def lexpr(self, p):
		return (p.lexpr)

	@_('lexpr ">" lexpr')
	def lexpr(self, p):
		return ('greater', p.lexpr0, p.lexpr1)
	
	@_('lexpr BIGGERE lexpr')
	def lexpr(self, p):
		return ('greater_equal', p.lexpr0, p.lexpr1)
	
	@_('lexpr "<" lexpr')
	def lexpr(self, p):
		return ("less", p.lexpr0, p.lexpr1)
	
	@_('lexpr SMALLERE lexpr')
	def lexpr(self, p):
		return ("less_equal", p.lexpr0, p.lexpr1)
	
	@_('lexpr EQUAL lexpr')
	def lexpr(self, p):
		return ("equal", p.lexpr0, p.lexpr1)

	@_('lexpr INEQUAL lexpr')
	def lexpr(self, p):
		return ("not_equal", p.lexpr0, p.lexpr1)

	@_('statement')
	def lexpr(self, p):
		return (p.statement)
	
	# --- arithmetic statement ---
	@_('expr')
	def statement(self, p):
		return (p.expr)
  
	@_('expr "+" expr')
	def expr(self, p):
		return ('add', p.expr0, p.expr1)
  
	@_('expr "-" expr')
	def expr(self, p):
		return ('sub', p.expr0, p.expr1)
  
	@_('expr "*" expr')
	def expr(self, p):
		return ('mul', p.expr0, p.expr1)
  
	@_('expr "/" expr')
	def expr(self, p):
		return ('div', p.expr0, p.expr1)
	
	@_('"(" expr ")"')
	def expr(self, p):
		return p.expr
	
	@_('"|" expr "|"')
	def expr(self, p):
		return ('abs', p.expr)
  
	@_('"-" expr %prec UMINUS')
	def expr(self, p):
		return p.expr
  
	@_('NAME')
	def expr(self, p):
		return ('var', p.NAME)
  
	@_('NUMBER')
	def expr(self, p):
		return ('num', p.NUMBER)
	
	# bool expr 
	

class TriggerEvaluate:
	
	def __init__(self, tree, env):
		self.env = env
		self.tree = tree
  
	def evaluate(self):
		result = self.walkTree(self.tree)
		return result

	def walkTree(self, node):
  
		if isinstance(node, bool):
			return node
  
		if node is None:
			return None
	
		# arithmetic operations 
		if node[0] == 'num':
			return node[1]
  
		if node[0] == 'add':
			return self.walkTree(node[1]) + self.walkTree(node[2])
		elif node[0] == 'sub':
			return self.walkTree(node[1]) - self.walkTree(node[2])
		elif node[0] == 'mul':
			return self.walkTree(node[1]) * self.walkTree(node[2])
		elif node[0] == 'div':
			return self.walkTree(node[1]) / self.walkTree(node[2])
		elif node[0] == 'abs':
			return abs(self.walkTree(node[1]))
  

		# logical operations 
		if node[0] == 'greater':
			return self.walkTree(node[1]) > self.walkTree(node[2])
		elif node[0] == 'greater_equal':
			return self.walkTree(node[1]) >= self.walkTree(node[2])
		elif node[0] == 'less':
			return self.walkTree(node[1]) < self.walkTree(node[2])
		elif node[0] == 'less_equal':
			return self.walkTree(node[1]) <= self.walkTree(node[2])
		elif node[0] == 'equal':
			return self.walkTree(node[1]) == self.walkTree(node[2])
		elif node[0] == 'not_equal':
			return self.walkTree(node[1]) != self.walkTree(node[2])
		
		# boolean operations 
		if node[0] == 'and':
			return self.walkTree(node[1]) and self.walkTree(node[2])
		elif node[0] == 'or':
			return self.walkTree(node[1]) or self.walkTree(node[2])
		elif node[0] == 'not':
			return not self.walkTree(node[1])

		if node[0] == 'var_assign':
			self.env[node[1]] = self.walkTree(node[2])
			return node[1]
  
		if node[0] == 'var':
			try:
				return self.env[node[1]]
			except LookupError:
				raise Exception("undefined variable '"+node[1]+"'")

def trigger_get_tree(expression):
	lexer = TriggerLexer()
	parser = TriggerParser()
	tree = parser.parse(lexer.tokenize(expression))
	return tree

def trigger_evaluate_tree(tree, env):
	exp = TriggerEvaluate(tree, env)
	return exp.evaluate()

def trigger_evaluate(expression, env):
	tree = None
	ret = None 
	try: 
		tree = trigger_get_tree(expression)
		ret = trigger_evaluate_tree(tree, env)
	except Exception as e:
		return (None, str(e))

	return (ret, "success") 

def trigger_verify(expression, env):
	tree = None
	ret = True 
	try: 
		tree = trigger_get_tree(expression)
		trigger_evaluate_tree(tree, env)
	except Exception as e:
		return (False, str(e))

	return (ret, "success") 


class LeotestTriggerMode:
	def __init__(self, fields, triggers, history=5, verify=False):
		"""init"""

		self.triggers = triggers
		self.env = {}
		self.trigger_trees = {}
		self.history = history

		if not verify:
			for trigger in self.triggers:
				self.trigger_trees[trigger] = trigger_get_tree(trigger)
				print(self.trigger_trees[trigger])
		
		for field in fields:
			self.env[field] = -1
			self.env["%s_avg" % field] = -1
			for i in range(1, history+1):
				self.env["%s_%d" % (field, i)] = -1
	
	def evaluate_triggers(self):
		"""evaluate here"""

		for trigger in self.triggers:
			ret = trigger_evaluate_tree(self.trigger_trees[trigger], self.env)
			print("Trigger='%s' eval='%s'" % (trigger, ret))
	
	def verify_triggers(self):
		"""verify triggers"""

		success = True
		err_msg = ""
		for trigger in self.triggers:
			ret, msg = trigger_verify(trigger, self.env)
			
			if not ret:
				success = False 
				err_msg = msg
				break 
		
		return (success, 'trigger: %s' % err_msg)

	def update_field(self, field, val):
		
		# update history 
		for i in range(2, self.history+1):
			self.env["%s_%d" % (field, i)] = self.env["%s_%d" % (field, i-1)]

		self.env["%s_1" % field] = self.env[field]
		self.env[field] = val

		sum = self.env[field]
		for i in range(1, self.history+1):
			sum += self.env["%s_%d" % (field, i)]
		
		self.env["%s_avg" % field] = int(sum / (self.history+1))

		self.evaluate_triggers()


def verify_trigger_default(trigger):

	trigger = LeotestTriggerMode(fields=FIELDS,
								triggers=[trigger],
								verify=True)
	return trigger.verify_triggers()

# print(trigger.evaluate_triggers())

# print(trigger.verify_triggers())
# expr = "|5-9| == 4"
# tree = trigger_get_tree(expr)
# print(trigger_evaluate(tree, {}))
			
			

